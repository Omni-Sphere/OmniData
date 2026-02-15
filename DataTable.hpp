#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <variant>
#include <vector>

#ifdef __GNUG__
#include <cxxabi.h>
#include <memory>
#endif

namespace omnicore::type {
class DataTable {
public:
  class Row {
  public:
    using Data =
        std::variant<int, double, bool, std::string, std::vector<uint8_t>>;
    using Value = std::optional<Data>;

    class ValueProxy {
    public:
      explicit ValueProxy(Value *valuePtr) : valuePtr_(valuePtr) {}

      static std::string Demangle(const char *name) {
#ifdef __GNUG__
        int status = 0;
        std::unique_ptr<char, void (*)(void *)> demangled(
            abi::__cxa_demangle(name, nullptr, nullptr, &status), std::free);
        return (status == 0 && demangled) ? demangled.get() : name;
#else
        return name;
#endif
      }

      struct TypeNameVisitor {
        std::string operator()(int) const { return "int"; }
        std::string operator()(double) const { return "double"; }
        std::string operator()(bool) const { return "bool"; }
        std::string operator()(const std::string &) const { return "string"; }
        std::string operator()(const std::vector<uint8_t> &) const {
          return "binary";
        }
      };

      static std::string GetTypeName(const Data &v) {
        return std::visit(TypeNameVisitor{}, v);
      }

      template <typename T> std::optional<T> GetOptional() const {
        if (IsNull())
          return std::nullopt;

        if (const T *val = std::get_if<T>(&valuePtr_->value()))
          return *val;

        const Row::Data &v = valuePtr_->value();
        throw std::runtime_error("DataTable: Cannot convert actual type '" +
                                 GetTypeName(v) +
                                 "' to requested optional type '" +
                                 Demangle(typeid(T).name()) + "'");
      }

      template <typename T> operator std::optional<T>() const {
        return GetOptional<T>();
      }

      template <typename T> operator T() const {
        if (IsNull()) {
          if constexpr (std::is_enum_v<T>) {
            return static_cast<T>(-1);
          } else {
            throw std::runtime_error("DataTable: Value is null");
          }
        }

        const Row::Data &v = valuePtr_->value();

        if constexpr (std::is_enum_v<T>) {
          if (const int *val = std::get_if<int>(&v))
            return static_cast<T>(*val);
          if (const double *dval = std::get_if<double>(&v))
            return static_cast<T>(static_cast<int>(*dval));
          if (const std::string *sval = std::get_if<std::string>(&v)) {
            try {
              return static_cast<T>(std::stoi(*sval));
            } catch (...) {
              return static_cast<T>(-1);
            }
          }
          return static_cast<T>(-1);
        } else {
          if (const T *val = std::get_if<T>(&v))
            return *val;

          // Support numeric promotions
          if constexpr (std::is_same_v<T, double>) {
            if (const int *ival = std::get_if<int>(&v))
              return static_cast<double>(*ival);
          } else if constexpr (std::is_same_v<T, int>) {
            if (const double *dval = std::get_if<double>(&v))
              return static_cast<int>(*dval);
          }

          throw std::runtime_error("DataTable: Cannot convert actual type '" +
                                   GetTypeName(v) + "' to requested type '" +
                                   Demangle(typeid(T).name()) + "'");
        }
      }

      operator bool() const {
        if (IsNull())
          throw std::runtime_error("DataTable: Value is null");

        const Row::Data &v = valuePtr_->value();
        if (const bool *val = std::get_if<bool>(&v))
          return *val;
        if (const int *val = std::get_if<int>(&v))
          return *val != 0;
        if (const double *val = std::get_if<double>(&v))
          return *val != 0.0;

        if (const std::string *val = std::get_if<std::string>(&v)) {
          if (*val == "Y" || *val == "y" || *val == "1" || *val == "true" ||
              *val == "T" || *val == "t")
            return true;
          if (*val == "N" || *val == "n" || *val == "0" || *val == "false" ||
              *val == "F" || *val == "f")
            return false;
          throw std::runtime_error(
              "DataTable: Invalid string value for bool conversion: " + *val);
        }

        throw std::runtime_error("DataTable: Unsupported type '" +
                                 GetTypeName(v) + "' for bool conversion");
      }

      bool IsNull() const { return !valuePtr_ || !valuePtr_->has_value(); }

    private:
      Value *valuePtr_;
    };

    void Set(const std::string &column, const Value &value);
    ValueProxy operator[](const std::string &column);
    const Value &operator[](const std::string &column) const;
    bool HasColumn(const std::string &column) const;

    std::unordered_map<std::string, Value>::iterator begin() {
      return values.begin();
    }
    std::unordered_map<std::string, Value>::iterator end() {
      return values.end();
    }
    std::unordered_map<std::string, Value>::const_iterator begin() const {
      return values.begin();
    }
    std::unordered_map<std::string, Value>::const_iterator end() const {
      return values.end();
    }

  private:
    std::unordered_map<std::string, Value> values;
  };

  void SetColumns(const std::vector<std::string> &cols);
  const std::vector<std::string> &GetColumns() const { return columns; }

  void AddRow(const Row &row);
  void Fill(const std::vector<Row> &newData);
  void Clear();

  Row &operator[](int index);
  const Row &operator[](int index) const;
  int RowsCount() const { return static_cast<int>(data.size()); }
  bool IsEmpty() const { return data.empty(); }

  std::vector<Row>::iterator begin() { return data.begin(); }
  std::vector<Row>::iterator end() { return data.end(); }
  std::vector<Row>::const_iterator begin() const { return data.begin(); }
  std::vector<Row>::const_iterator end() const { return data.end(); }

private:
  std::vector<std::string> columns;
  std::vector<Row> data;
};
} // namespace omnicore::type
