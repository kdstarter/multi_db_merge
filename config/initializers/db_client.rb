module DbClient
  def logger
    @logger ||= Logger.new('log/debug.log')
  end

  def larger(&block)
    puts '---larger---'
    ActiveRecord::Base.connected_to(role: :larger) do
      yield
    end
  end

  def query(which, sql)
    # puts "---query_#{which}---"
    self.send("query_#{which}", sql)
  end

  def query_larger(sql)
    puts '---query_larger---' if @larger_conn.blank?
    @larger_conn ||= ActiveRecord::Base.establish_connection(Rails.application.config.database_configuration[Rails.env]['larger']).connection
    begin
      @larger_conn.execute(sql)
    rescue ActiveRecord::StatementInvalid => e
      if e.inspect.include?('PG::ConnectionBad')
        puts "largerWarn: #{e.inspect}"
        @larger_conn.reconnect!
        @larger_conn.execute(sql)
      else
        raise e
      end
    end
  end

  def smaller(&block)
    puts '---smaller---'
    ActiveRecord::Base.connected_to(role: :smaller) do
      yield
    end
  end

  def query_smaller(sql)
    puts '---query_smaller---' if @smaller_conn.blank?
    @smaller_conn ||= ActiveRecord::Base.establish_connection(Rails.application.config.database_configuration[Rails.env]['smaller']).connection
    begin
      @smaller_conn.execute(sql)
    rescue ActiveRecord::StatementInvalid => e
      if e.inspect.include?('PG::ConnectionBad')
        puts "smallerError: #{e.inspect}"
        @smaller_conn.reconnect!
        @smaller_conn.execute(sql)
      else
        raise e
      end
    end
  end
end

include DbClient
