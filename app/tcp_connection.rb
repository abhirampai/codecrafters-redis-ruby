class TcpConnection
  class << self
    attr_reader :socket, :parser, :host, :port, :server
    
    def send_handshake(host, port, server)
      @host = host
      @port = port
      @server = server
      @socket = establish_connection
      @parser = RESPParser.new("")
      send_ping_message
      send_replconf_message
      send_psync_message
    rescue StandardError, EOFError
    end

    private
 
    def establish_connection
      socket = TCPSocket.open(host, port)
      server.sockets << socket
      socket
    end

    def send_ping_message
      socket.write(parser.encode(["PING"], "array"))
      socket.readpartial(1024)
    end

    def send_replconf_message
      socket.write(parser.encode(["REPLCONF", "listening-port", server.port], "array"))
      socket.readpartial(1024)
      socket.write(parser.encode(["REPLCONF", "capa", "psync2"], "array"))
      socket.readpartial(1024)
    end

    def send_psync_message
      socket.write(parser.encode(["PSYNC", "?", "-1"], "array"))
    end
  end
end
