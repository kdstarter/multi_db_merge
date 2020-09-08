class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
  connects_to database: { larger: :larger, smaller: :smaller }

end
