require "securerandom"
require_relative "resp_parser"

class CommandHandler
  attr_reader :command, :messages, :client, :setter, :parser, :data_size, :server

  def initialize(command, messages, client, setter, parser, data_size, server)
    @command = command
    @messages = messages
    @client = client
    @setter = setter
    @parser = parser
    @data_size = data_size
    @server = server
  end

  def handle
    case command.downcase
    when "ping"
      client.write("+PONG\r\n") if server.replica == false
    when "echo"
      messages.each do |message|
        client.write(parser.encode(message, "bulk_string"))
      end
    when "set"
      @setter[messages[0]] = { data: messages[1], created_at: Time.now, ttl: -1, type: "string" }

      if messages.length > 2
        @setter[messages[0]][:ttl] = messages[3].to_i if messages[2].downcase == "px"
      end
      
      if server.replica == false
        client.write(parser.encode("OK", "simple_string"))
        server.send_buffer_message(["SET", messages[0], messages[1]])
      end
    when "get"
      key = messages[0]
      if setter.has_key?(key)
        if setter[key][:ttl] == -1
          client.write(parser.encode(setter[key][:data], "bulk_string"))
        else
          ellapsed_time_in_milliseconds = ((Time.now - setter[key][:created_at]) * 1000).to_f
          if ellapsed_time_in_milliseconds < setter[key][:ttl]
            client.write(parser.encode(setter[key][:data], "bulk_string"))
          else
            client.write(parser.encode("", "bulk_string"))
          end
        end
      else
        client.write(parser.encode("", "bulk_string"))
      end
    when "config"
      sub_command = messages[0].downcase
      if sub_command == "get"
        key = messages[1].downcase
        if key == "dbfilename"
          client.write(parser.encode(["dbfilename", server.dbfilename], "array"))
        elsif key == "dir"
          client.write(parser.encode(["dir", server.dir], "array"))
        end
      end
    when "keys"
      sub_command = messages[0].downcase
      if sub_command == "*"
        client.write(parser.encode(@setter.keys, "array"))
      end
    when "info"
      sub_command = messages[0].downcase
      if sub_command == "replication"
        role = server.replica ? "slave" : "master"
        master_repl_offset = 0
        message = "role:#{role}\nmaster_replid:#{server.master_replid}\nmaster_repl_offset:#{master_repl_offset}"
        client.write(parser.encode(message, "bulk_string"))
      end
    when "replconf"
      sub_command = messages[0].downcase
      if sub_command == "listening-port"
        server.replicas.concat([client])
        client.write(parser.encode("OK", "simple_string"))
      elsif sub_command == "getack"
        p "inside getack"
        client.write(parser.encode(["REPLCONF", "ACK", server.commands_processed_in_bytes.to_s], "array"))
      elsif sub_command == "ack"
        server.update_replicas_ack(false) if server.replica == false
      else
        client.write(parser.encode("OK", "simple_string"))
      end
    when "psync"
      client.write(parser.encode("FULLRESYNC #{server.master_replid} 0", "simple_string"))
      empty_rdb_file = File.open("app/empty_rdb.rdb", "rb")
      content = [empty_rdb_file.read(1024).strip].pack("H*")
      client.write("$#{content.size}\r\n")
      client.write(content)
    when "wait"
      client.write(parser.encode(0, "integer")) if server.replicas.length.zero?
      wait_time = Time.now + messages[1].to_f / 1000
      if server.replicas.length.positive?
        server.send_buffer_message(["REPLCONF", "GETACK", "*"])
      end
      while server.replicas_ack <= messages[0].to_i && Time.now < wait_time
        sleep(0.1)
      end
      message = server.replicas_ack.positive? ? server.replicas_ack : server.replicas.length
      server.update_replicas_ack(true, 0)
      client.write(parser.encode(message, "integer"))
    when "type"
      if setter.has_key?(messages[0])
        client.write(parser.encode(setter[messages[0]][:type], "simple_string"))
      else
       client.write(parser.encode("none", "simple_string"))
      end
    when "xadd"
      key = messages[0]
      id = auto_generate_id(key, messages[1])

      hash = { id: id }
      fields = messages[2..]
      fields.each_slice(2) do |key, value|
        hash[key] = value
      end
      if setter.has_key?(key)
        previous_milliseconds, previous_sequence = setter[key][:data].last[:id].split("-").map(&:to_i)
        data = setter[key][:data].append(hash)
        milliseconds, sequence = hash[:id].split("-").map(&:to_i)
        return unless valid_data(milliseconds, sequence, previous_milliseconds, previous_sequence)

        setter[key] = { data: data, created_at: Time.now, ttl: -1, type: "stream" }
      else
        milliseconds, sequence = hash[:id].split("-").map(&:to_i)
        if milliseconds.zero? && sequence.zero?
          return raise_stream_error("ERR The ID specified in XADD must be greater than 0-0")
        end

        setter[key] = { data: [hash], created_at: Time.now, ttl: -1, type: "stream" }
      end
      client.write(parser.encode(id, "bulk_string"))
    when "xrange"
      key = messages[0]
      start_id = messages[1]
      end_id = messages[2]
      copy_item = false
      data_range = []
      if start_id == "-"
        start_id = setter[key][:data].first[:id]
      end
      setter[key][:data].each do |item|
        if item[:id].include?(start_id)
          copy_item = true
          data_range << item
          next
        end
        if item[:id].include?(end_id)
          data_range << item
          break
        end
        if copy_item
          data_range << item
        end
      end
      result = data_range.map do |item|
        [item[:id], *item.except(:id).entries]
      end
      client.write(parser.encode(result, "array"))
    end
    update_commands_processed
  end
  
  def update_commands_processed
    server.commands_processed_in_bytes += data_size if server.replica != false 
  end
  
  def raise_stream_error(message)
    client.write(parser.encode(message, "simple_error"))
  end
  
  def valid_data(milliseconds, sequence, previous_milliseconds = 0, previous_sequence = 0)
    if milliseconds.zero? && sequence.zero?
      raise_stream_error("ERR The ID specified in XADD must be greater than 0-0")
      return false
    elsif previous_milliseconds > milliseconds
      raise_stream_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")
      return false
    elsif previous_milliseconds == milliseconds && previous_sequence >= sequence
      raise_stream_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")
      return false
    else
      return true
    end
  end
  
  def auto_generate_id(key, id)
    timestamp, sequence = id.split("-")
    if timestamp == "*"
      timestamp = (Time.now.to_f * 1000).to_i
      sequence = "0"
    end

    if sequence == "*"
      if setter.has_key?(key)
        data = setter[key][:data]
        data_with_same_timestamp = data.select { |k| k[:id].split("-").first == timestamp }

        if !data_with_same_timestamp.empty?
          sequence = (data_with_same_timestamp.last[:id].split("-").last.to_i + 1).to_s
        else
          sequence = "0"
        end
      else
        sequence = "1"
      end
    end

    return "#{timestamp}-#{sequence}"
  end
end


