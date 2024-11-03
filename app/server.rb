require "socket"
require_relative "resp_parser"

class RedisServer
  attr_reader :server, :clients
  def initialize(port)
    @server = TCPServer.new(port)
    @clients = []
  end

  def listen
    loop do
      accept_incoming_connections
    end
  end

  private

  def accept_incoming_connections
    begin
      fds_to_watch = [@server, *@clients]
      ready_to_read, _, _ = IO.select(fds_to_watch)
      ready_to_read.each do |client|
        if client == @server
          @clients << @server.accept
        else
          handle_client(client)
        end
      end
    rescue IO::WaitReadable, Errno::EINTR
    end
  end

  def handle_client(client)
    data = client.readpartial(1024)
    parser = RESPParser.new(data)
    parsed_data = parser.parse
    command, message = parsed_data[:data]
    case command
    when "PING"
      client.write("+PONG\r\n")
    when "ECHO"
      client.write(parser.encode(message, "bulk_string"))
    end
  rescue StandardError
  end
end

redis_cli = RedisServer.new(6379).listen
