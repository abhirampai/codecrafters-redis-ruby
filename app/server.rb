require "socket"

class YourRedisServer
  def initialize(port)
    @port = port
  end

  def start
    server = TCPServer.new(@port)
    client = server.accept
  end
end

redis_cli = YourRedisServer.new(6379).start

while line = redis_cli.gets
  if(line.chomp.include?("PING"))
    redis_cli.puts "+PONG\r\n"
  end
end
