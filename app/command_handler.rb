class CommandHandler
  attr_reader :command, :messages, :client, :setter, :parser

  def initialize(command, messages, client, setter, parser)
    @command = command
    @messages = messages
    @client = client
    @setter = setter
    @parser = parser
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
          if ellapsed_time_in_milliseconds < setter[key][:ttl]
            client.write(parser.encode(setter[key][:data], "bulk_string"))
          else
            client.write(parser.encode("", "bulk_string"))
          end
        end
      else
        client.write(parser.encode("", "bulk_string"))
      end
    end
  end
end
