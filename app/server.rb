require "socket"

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
    client.readpartial(1024)
    client.write("+PONG\r\n")
  rescue EOFError
  end
end

redis_cli = RedisServer.new(6379).listen
