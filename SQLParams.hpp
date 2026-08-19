#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>
#include <cstdint>

namespace omnisphere::types
{
    using SQLParam = std::variant<std::monostate, bool, int, std::string,
    std::vector<uint8_t>, double>;

    template <typename T> struct is_optional : std::false_type {};
    template <typename T> struct is_optional<std::optional<T>> : std::true_type {};

    template <typename T>
    inline SQLParam MakeSQLParam(const std::optional<T> &opt)
    {
        if (opt.has_value())
            return SQLParam
        {*opt};
        else
            return SQLParam
        {std::monostate{}};
    }

    inline SQLParam MakeSQLParam(int val)
    { return SQLParam
        {val}; }

    inline SQLParam MakeSQLParam(const std::string &val)
    { return SQLParam
        {val}; }

    inline SQLParam MakeSQLParam(const std::vector<uint8_t> &val)
    {
        return SQLParam
        {val};
    }

    inline SQLParam MakeSQLParam(bool val)
    { return SQLParam
        {val}; }

    inline SQLParam MakeSQLParam(double val)
    { return SQLParam
        {val}; }

    template <typename T, typename = std::enable_if_t<std::is_enum_v<T>>>
    inline SQLParam MakeSQLParam(T val)
    {
        return SQLParam
        {static_cast<std::underlying_type_t<T>>(val)};
    }

    inline std::string FormatSQL(const std::string& query, const std::vector<SQLParam>& params)
    {
        std::string result = query;
        size_t pos = 0;

        for (const auto& param : params)
        {
            pos = result.find('?', pos);

            if (pos == std::string::npos) break;

            std::string val;
            std::visit([&val](auto&& arg)
            {
                       using T = std::decay_t<decltype(arg)>;

                       if constexpr (std::is_same_v<T, std::monostate>)
                       val = "NULL";
                       else if constexpr (std::is_same_v<T, bool>)
                       val = (arg ? "true" : "false"); // native bool — no Y/N
                       else if constexpr (std::is_same_v<T, int>)
                       val = std::to_string(arg);
                       else if constexpr (std::is_same_v<T, double>)
                       val = std::to_string(arg);
                       else if constexpr (std::is_same_v<T, std::string>)
                {
                       std::string s = arg;
                       size_t p = 0;

                       while ((p = s.find("'", p)) != std::string::npos)
                    {
                       s.replace(p, 1, "''");
                       p += 2;
                    }
                       val = "'" + s + "'";
                }
                       else if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
                       val = "0x<BINARY>";
            }, param);

            result.replace(pos, 1, val);
            pos += val.length();
        }

        return result;
    }

    inline std::string FormatSQL(const std::string& query, const std::vector<std::string>& params)
    {
        std::vector<SQLParam> sqlParams;

        for (const auto& p : params) sqlParams.push_back(SQLParam
        {p});

        return FormatSQL(query, sqlParams);
    }

} // namespace omnisphere::types