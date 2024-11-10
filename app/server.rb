require "socket"
require_relative "resp_parser"
require_relative "command_handler"

class RedisServer
  attr_reader :server, :clients, :setter, :dir, :dbfilename, :replica, :port
  def initialize(arguments)
    @clients = []
    @setter = Hash.new
    @replica = false
    parse_arguments(arguments)
    set_default_port if !server
    populate_setter_with_rdb_file_data if dir && dbfilename
    send_handshake_message if replica
  end

  def listen
    loop do
      accept_incoming_connections
    end
  end

  private

  def set_default_port
    @port = 6379
    @server = TCPServer.new(port)
  end

  def parse_arguments(arguments)
    arguments.each_with_index do |argument, arg_index|
      if argument == "--dir"
        @dir = arguments[arg_index + 1]
      elsif argument == "--dbfilename"
        @dbfilename = arguments[arg_index + 1]
      elsif argument == "--port"
        @port = arguments[arg_index + 1]
        @server = TCPServer.new(port)
      elsif argument == "--replicaof"
        host, port = arguments[arg_index + 1].split(" ")
        @replica = { host: host, port: port }
      end
    end
  end
  
  def populate_setter_with_rdb_file_data
    return unless File.file?(File.join(dir, dbfilename))

    current_unix_time_stamp = Time.now.to_i
    file = File.open(File.join(dir, dbfilename), "rb")
    file.seek(9) # skip header section
    loop do
      opCode = file.read(1)
      case opCode.unpack1("H*").to_sym
      when :fb
        size_of_hash_table = file.read(1).unpack1("C*")
        size_of_expiry_table = file.read(1).unpack1("C*")
        if size_of_expiry_table > 0
          size_of_expiry_table.times do |_|
            type_of_expiry_time = file.read(1).unpack1("H*").to_sym
            case type_of_expiry_time
            when :fc
              expiry_time = file.read(8).unpack1("V")
            when :fd
              expiry_time = file.read(4).unpack1("V") * 1000
            end
            value_encoding_type_or_expiry_time = file.read(1).unpack1("C*") # skip for now
            size_of_key = file.read(1).unpack1("C*")
            key = file.read(size_of_key)
            size_of_value = file.read(1).unpack1("C*")
            value = file.read(size_of_value)
            if current_unix_time_stamp < expiry_time
              @setter[key] = { data: value, created_at: Time.now, ttl: expiry_time }
            end
          end
        end
        (size_of_hash_table - size_of_expiry_table).times do |_|
          value_encoding_type_or_expiry_time = file.read(1).unpack1("C*") # skip for now
          size_of_key = file.read(1).unpack1("C*")
          key = file.read(size_of_key)
          size_of_value = file.read(1).unpack1("C*")
          value = file.read(size_of_value)
          @setter[key] = { data: value, created_at: Time.now, ttl: -1 }
        end
      when :ff
        break
      end 
    end
    p @setter
    file.close
  end

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
    command, *messages = parsed_data[:data]
    command_handler = CommandHandler.new(command, messages, client, setter, parser, self)

    command_handler.handle
  rescue StandardError
  end

  def send_handshake_message
    socket = TCPSocket.open(replica[:host], replica[:port])
    parser = RESPParser.new("")
    socket.write(parser.encode(["PING"], "array"))
    socket.readpartial(1024)
    socket.write(parser.encode(["REPLCONF", "listening-port", port], "array"))
    socket.readpartial(1024)
    socket.write(parser.encode(["REPLCONF", "capa", "psync2"], "array"))
    socket.readpartial(1024)
  end
end

redis_cli = RedisServer.new(ARGV[0..]).listen
