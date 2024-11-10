class CommandHandler
  attr_reader :command, :messages, :client, :setter, :parser, :server

  def initialize(command, messages, client, setter, parser, server)
    @command = command
    @messages = messages
    @client = client
    @setter = setter
    @parser = parser
    @server = server
  end

  def handle
    case command.downcase
    when "ping"
      client.write("+PONG\r\n")
    when "echo"
      messages.each do |message|
        client.write(parser.encode(message, "bulk_string"))
      end
    when "set"
      @setter[messages[0]] = { data: messages[1], created_at: Time.now, ttl: -1 }

      if messages.length > 2
        @setter[messages[0]][:ttl] = messages[3].to_i if messages[2].downcase == "px"
      end

      client.write(parser.encode("OK", "simple_string"))
    when "get"
      key = messages[0]
      if setter.has_key?(key)
        if setter[key][:ttl] == -1
          client.write(parser.encode(setter[key][:data], "bulk_string"))
        else
          ellapsed_time_in_milliseconds = ((Time.now - setter[key][:created_at]) * 1000).to_f
          p ellapsed_time_in_milliseconds
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
        p server.replica
        if server.replica
          client.write(parser.encode("role:slave", "bulk_string"))
        else
          client.write(parser.encode("role:master", "bulk_string"))
        end
      end
    end
  end
end
