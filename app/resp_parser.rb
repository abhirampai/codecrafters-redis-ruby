class RESPParser
  attr_reader :resp, :current_index, :result

  def initialize(resp)
    @result = {}
    @current_index = 0
    @resp = resp
  end

  def parse
    case resp[current_index]
    when "+"
      @current_index += 1
      @result = { data: parse_simple_string, type: "simple_string" }
    when "-"
      @current_index += 1
      @result = { data: parse_error, type: "simple_error" }
    when ":"
      @current_index += 1
      @result = { data: parse_integer, type: "integer" }
    when "$"
      @current_index += 1
      @result = { data: parse_bulk_string, type: "bulk_string" }
    when "*"
      @current_index += 1
      @result = { data: parse_array, type: "array" }
    when "_"
      @current_index += 3
      @result = { data: nil, type: "nil" }
    when "#"
      @current_index += 1
      @result = { data: parse_boolean, type: "boolean" }
    when ","
      @current_index += 1
      @result = { data: parse_doubles, type: "double" }
    when "("
      @current_index += 1
      @result = { data: parse_big_number, type: "big_number" }
    when "!"
      @current_index += 1
      @result = { data: parse_bulk_errors, type: "bulk_errors" }
    when "="
      @current_index += 1
      @result = { data: parse_verbatim_string, type: "verbatim_string" }
    when "%"
      @current_index += 1
      @result = { data: parse_map, type: "map" }
    when "|"
      @current_index += 1
      @result = { data: parse_map, type: "attribute" }
    when "~"
      @current_index += 1
      @result = { data: Set.new(parse_array), type: "set" }
    when ">"
      @current_index += 1
      @result = { data: parse_array, type: "push" }
    else
      raise "Unknown parse type refer to redis protocol (https://redis.io/docs/latest/develop/reference/protocol-spec)"
    end
  end
  
  def encode(data, type)
    case type
    when "simple_string"
      "+#{data}\r\n"
    when "simple_error"
      "-"#{data}\r\n"
    when "bulk_string"
      data_length = data.length.zero? ? -1 : data.length
      "$#{data_length}\r\n#{data}\r\n"
    when "integer"
      ":#{data}\r\n"
    when "double"
      ",#{data}\r\n"
    when "array"
      "*#{data.length}\r\n"
      data.each do |element|
        encode(element)
      end
      "\r\n"
    when "set"
      "~#{data.length}\r\n"
      data.each do |element|
        encode(element)
      end
      "\r\n"
    when "map"
      "%#{data.length}\r\n"
      data.each do |key, value|
        self.encode(key)
        self.encode(value)
      end
      "\r\n"
    else
      raise "Unknown encode type refer to redis protocol (https://redis.io/docs/latest/develop/reference/protocol-spec)"
    end
  end

  def parse_simple_string
    data = resp[current_index..].split("\r\n").first
    @current_index += data.length + 3
    data
  end

  def parse_error
    data = resp[current_index..].split("\r\n").first
    @current_index += data.length + 3
    data
  end

  def parse_integer
    data = resp[current_index..].split("\r\n").first.to_i
    @current_index += data.length + 3
    data
  end

  def parse_bulk_string
    length = resp[current_index].to_i
    data = resp[current_index + 3..length + current_index + 2]
    @current_index += length + 5
    data
  end

  def parse_array
    number_of_elements = resp[current_index].to_i
    @current_index += 3
    array = []
    number_of_elements.times do
      array << parse[:data]
    end
    @current_index += 2
    array
  end

  def parse_boolean
    data = resp[current_index].to_s.downcase! == "t"
    @current_index += 3
    data
  end

  def parse_doubles
    data = resp[current_index..].split("\r\n").first.to_f
    @current_index += data.length + 3
    data
  end

  def parse_big_number
    data = resp[current_index..].split("\r\n").first
    @current_index += data.length + 3
    data
  end

  def parse_bulk_errors
    number_of_elements = resp[current_index].to_i
    @current_index += 3
    array = []
    number_of_elements.times do
      data = resp[current_index..].split("\r\n").first
      array << data
      @current_index += data.length + 2
    end
    array
  end

  def parse_verbatim_string
    length = resp[current_index].to_i
    data = resp[current_index + 3..length + current_index + 2]
    @current_index += length + 5
    data
  end

  def parse_map
    number_of_elements = resp[current_index].to_i
    @current_index += 3
    map = {}
    number_of_elements.times do
      key = parse[:data]
      value = parse[:data]
      map[key] = value
    end
    @current_index += 2
    map
  end
end
