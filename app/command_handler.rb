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
    p "Handling command :=> #{command}"
    case command.downcase
    when "ping"
      client.write("+PONG\r\n") if server.replica == false
    when "echo"
      messages.each do |message|
        client.write(parser.encode(message, "bulk_string"))
      end
    when "set"
      @setter[messages[0]] = { data: messages[1], created_at: Time.now, ttl: -1 }

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
        client.write(parser.encode(["REPLCONF", "ACK", server.commands_processed_in_bytes.to_s], "array"))
      else
        client.write(parser.encode("OK", "simple_string"))
      end
    when "psync"
      client.write(parser.encode("FULLRESYNC #{server.master_replid} 0", "simple_string"))
      empty_rdb_file = File.open("app/empty_rdb.rdb", "rb")
      content = [empty_rdb_file.read(1024).strip].pack("H*")
      client.write("$#{content.size}\r\n")
      client.write(content)
    end
    update_commands_processed
  end
  
  def update_commands_processed
    server.commands_processed_in_bytes += data_size if server.replica != false 
  end
end


