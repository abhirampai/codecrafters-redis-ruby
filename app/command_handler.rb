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
      @setter[messages[0]] = messages[1]
      client.write(parser.encode("OK", "simple_string"))
    when "get"
      p "messages: #{messages}, setter: #{setter}"
      if setter.has_key?(messages[0])
        client.write(parser.encode(setter[messages[0]], "bulk_string"))
      else
        client.write(parser.encode("", "bulk_string"))
      end
    end
  end
end
