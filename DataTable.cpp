#include "DataTable.hpp"
#include <stdexcept>

namespace omnicore::type {
void DataTable::SetColumns(const std::vector<std::string> &cols) {
  columns = cols;
}

void DataTable::AddRow(const Row &row) { data.push_back(row); }

void DataTable::Fill(const std::vector<Row> &newData) {
  data.insert(data.end(), newData.begin(), newData.end());
}

void DataTable::Clear() { data.clear(); }

DataTable::Row &DataTable::operator[](int index) {
  if (index < 0 || index >= static_cast<int>(data.size())) {
    throw std::out_of_range(
        "DataTable: Row index " + std::to_string(index) +
        " is out of range (size: " + std::to_string(data.size()) + ")");
  }
  return data[index];
}

const DataTable::Row &DataTable::operator[](int index) const {
  if (index < 0 || index >= static_cast<int>(data.size())) {
    throw std::out_of_range(
        "DataTable: Row index " + std::to_string(index) +
        " is out of range (size: " + std::to_string(data.size()) + ")");
  }
  return data[index];
}

void DataTable::Row::Set(const std::string &column, const Value &value) {
  values[column] = value;
}

DataTable::Row::ValueProxy
DataTable::Row::operator[](const std::string &column) {
  std::unordered_map<std::string, Value>::iterator it = values.find(column);
  if (it == values.end()) {
    throw std::runtime_error("DataTable: Column '" + column +
                             "' not found in row");
  }
  return ValueProxy(&it->second);
}

const DataTable::Row::Value &
DataTable::Row::operator[](const std::string &column) const {
  std::unordered_map<std::string, Value>::const_iterator it =
      values.find(column);
  if (it == values.end()) {
    throw std::runtime_error("DataTable: Column '" + column +
                             "' not found in row");
  }
  return it->second;
}

bool DataTable::Row::HasColumn(const std::string &column) const {
  return values.find(column) != values.end();
}
} // namespace omnicore::type