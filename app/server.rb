require "socket"
require_relative "resp_parser"
require_relative "command_handler"

class RedisServer
  attr_reader :server, :clients, :setter, :dir, :dbfilename
  def initialize(port, arguments)
    @server = TCPServer.new(port)
    @clients = []
    @setter = Hash.new
    parse_arguments(arguments)
    populate_setter if dir && dbfilename
  end

  def listen
    loop do
      accept_incoming_connections
    end
  end

  private

  def parse_arguments(arguments)
    arguments.each_with_index do |argument, arg_index|
      if argument == "--dir"
        @dir = arguments[arg_index + 1]
      elsif argument == "--dbfilename"
        @dbfilename = arguments[arg_index + 1]
      end
    end
  end
  
  def populate_setter
    return unless File.file?(File.join(dir, dbfilename))

    file = File.open(File.join(dir, dbfilename), "rb")
    file.seek(9) # skip header section
    loop do
      opCode = file.read(1)
      case opCode.unpack1("H*").to_sym
      when :fb
        size_of_hash_table = file.read(1).unpack1("C*")
        size_of_expiry_table = file.read(1).unpack1("C*")
        size_of_hash_table.times do |_|
          value_encoding_type = file.read(1).unpack1("C*") # skip for now
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
end

redis_cli = RedisServer.new(6379, ARGV[0..]).listen
