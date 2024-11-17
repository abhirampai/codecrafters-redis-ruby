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
      @result = { data: parse_simple_string, type: "simple_string", current_index: current_index }
    when "-"
      @current_index += 1
      @result = { data: parse_error, type: "simple_error", current_index: current_index }
    when ":"
      @current_index += 1
      @result = { data: parse_integer, type: "integer", current_index: current_index }
    when "$"
      @current_index += 1
      @result = { data: parse_bulk_string, type: "bulk_string", current_index: current_index }
    when "*"
      @current_index += 1
      @result = { data: parse_array, type: "array", current_index: current_index }
    when "_"
      @current_index += 3
      @result = { data: nil, type: "nil", current_index: current_index }
    when "#"
      @current_index += 1
      @result = { data: parse_boolean, type: "boolean", current_index: current_index }
    when ","
      @current_index += 1
      @result = { data: parse_doubles, type: "double", current_index: current_index }
    when "("
      @current_index += 1
      @result = { data: parse_big_number, type: "big_number", current_index: current_index }
    when "!"
      @current_index += 1
      @result = { data: parse_bulk_errors, type: "bulk_errors", current_index: current_index }
    when "="
      @current_index += 1
      @result = { data: parse_verbatim_string, type: "verbatim_string", current_index: current_index }
    when "%"
      @current_index += 1
      @result = { data: parse_map, type: "map", current_index: current_index }
    when "|"
      @current_index += 1
      @result = { data: parse_map, type: "attribute", current_index: current_index }
    when "~"
      @current_index += 1
      @result = { data: Set.new(parse_array), type: "set", current_index: current_index }
    when ">"
      @current_index += 1
      @result = { data: parse_array, type: "push", current_index: current_index }
    else
      p "Unknown parse type refer to redis protocol (https://redis.io/docs/latest/develop/reference/protocol-spec)"
    end
  end

  def encode(data, type)
    case type
    when "simple_string"
      "+#{data}\r\n"
    when "simple_error"
      "-#{data}\r\n"
    when "bulk_string"
      return "$-1\r\n" if data.length.zero?

      "$#{data.length}\r\n#{data}\r\n"
    when "integer"
      ":#{data}\r\n"
    when "double"
      ",#{data}\r\n"
    when "array"
      "*#{data.length}\r\n#{data.map { |d| encode(d, "bulk_string") }.join("")}"
    when "set"
      "~#{data.length}\r\n#{data.map { |d| encode(d, "bulk_string") }.join("")}"
    when "map"
      data_map = data.map { |k, v| encode(k, "bulk_string") + encode(v, "bulk_string") }.join("")
      "%#{data.length}\r\n#{data_map}"
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
    data = resp[current_index..].split("\r\n").first
    @current_index += data.length + 3
    data.to_i
  end

  def parse_bulk_string
    length = resp[current_index..].split("\r\n").first.to_i
    start_index = current_index + length.digits.size + 2
    end_index = start_index + length - 1
    data = resp[start_index..end_index]
    @current_index = end_index + 3
    data
  end

  def parse_array
    number_of_elements = resp[current_index..].split("\r\n").first.to_i
    @current_index += number_of_elements.digits.size + 2
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
    data = resp[current_index..].split("\r\n").first
    @current_index += data.length + 3
    data.to_f
  end

  def parse_big_number
    data = resp[current_index..].split("\r\n").first
    @current_index += data.length + 3
    data.to_i
  end

  def parse_bulk_errors
    number_of_elements = resp[current_index..].split("\r\n").first.to_i
    @current_index += number_of_elements.digits.size + 2
    array = []
    number_of_elements.times do
      data = resp[current_index..].split("\r\n").first
      array << data
      @current_index += data.length + 2
    end
    array
  end

  def parse_verbatim_string
    length = resp[current_index..].split("\r\n").first.to_i
    start_index = current_index + length.digits.size + 2
    end_index = start_index + length - 1
    data = resp[current_index + 3..length + current_index + 2]
    @current_index += end_index + 3
    data
  end

  def parse_map
    number_of_elements = resp[current_index..].split("\r\n").first.to_i
    @current_index += number_of_elements.digits.size + 2
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
