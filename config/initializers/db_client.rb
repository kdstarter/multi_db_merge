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
    @larger_conn.execute(sql)
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
    @smaller_conn.execute(sql)
  end
end

include DbClient
