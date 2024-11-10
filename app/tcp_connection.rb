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
      initialize_data_event_listener
    rescue StandardError, EOFError
    end

    private

    def initialize_data_event_listener
      Thread.new do
        begin
          fds_to_watch = [socket]
          ready_to_read, _, _ = IO.select(fds_to_watch)
          ready_to_read.each do |client|
           listen_to_messages(client)
          end
        end
      end
    rescue IO::WaitReadable, Errno::EINTR, StandardError
    end

    def listen_to_messages(client)
      data = client.readpartial(1024)
      current_index = 0
      if data
        while current_index < data.length
          message = RESPParser.new(data[current_index..]) 
          parsed_message = message.parse
          command, *messages = parsed_message[:data]
          command_handler = CommandHandler.new(command, messages, server.server, server.setter, message, server)

          command_handler.handle
          current_index += parsed_message[:current_index] - 2
        end
      end
    rescue EOFError
    end

    def establish_connection
      TCPSocket.open(host, port)
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
      socket.readpartial(1024)
      socket.readpartial(1024)
    end
  end
end
