#pragma once
#include "SQLParams.hpp"
#include <boost/describe.hpp>
#include <boost/mp11.hpp>
#include <optional>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <cctype>

namespace omnisphere::types
{
    template <typename T>
    struct is_vector : std::false_type {};

    template <typename U>
    struct is_vector<std::vector<U>> : std::true_type {};

    template <typename Model>
    inline std::vector<std::string> FilterModelFields(const std::vector<std::string>& requestedFields)
    {
        std::unordered_map<std::string, std::string> modelColumns;
        std::vector<std::string> defaultColumns;

        boost::mp11::mp_for_each<boost::describe::describe_members<Model, boost::describe::mod_public>>(
            [&](auto D) {
                std::string rawName = D.name;

                using FieldType = std::remove_cvref_t<decltype(std::declval<Model>().*D.pointer)>;

                if constexpr (is_vector<FieldType>::value) {
                    return;
                }

                std::string pascalName = rawName;
                if (!pascalName.empty() && std::islower(static_cast<unsigned char>(pascalName[0]))) {
                    pascalName[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(pascalName[0])));
                }

                std::string lowerKey = rawName;
                std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);

                std::string formattedCol = "\"" + pascalName + "\"";
                modelColumns[lowerKey] = formattedCol;
                defaultColumns.push_back(formattedCol);
            }
        );

        if (requestedFields.empty())
        {
            return defaultColumns;
        }

        std::vector<std::string> filtered;
        filtered.reserve(requestedFields.size());

        auto stripQuotesAndLower = [](const std::string& s) -> std::string {
            std::string res = s;
            if (res.size() >= 2 && res.front() == '"' && res.back() == '"') {
                res = res.substr(1, res.size() - 2);
            }
            std::transform(res.begin(), res.end(), res.begin(), ::tolower);
            return res;
        };

        for (const auto& req : requestedFields)
        {
            std::string cleanKey = stripQuotesAndLower(req);
            auto it = modelColumns.find(cleanKey);
            if (it != modelColumns.end())
            {
                filtered.push_back(it->second);
            }
        }

        if (filtered.empty())
        {
            return defaultColumns;
        }

        return filtered;
    }

    struct ColumnValue {
        std::string Column;
        SQLParam Value;
    };

    struct UpdateQueryResult {
        std::string Query;
        std::vector<SQLParam> Parameters;
    };

    template <typename T>
    inline void BindOptional(std::vector<ColumnValue>& cols, const std::string& columnName, const std::optional<T>& optVal)
    {
        if (optVal.has_value())
        {
            cols.push_back({columnName, MakeSQLParam(optVal.value())});
        }
    }

    template <class DTO>
    inline std::vector<ColumnValue> ExtractUpdateColumns(const DTO& dto)
    {
        std::vector<ColumnValue> cols;
        boost::mp11::mp_for_each<boost::describe::describe_members<DTO, boost::describe::mod_public>>(
            [&](auto PropertyDescriptor) {
                std::string colName = PropertyDescriptor.name;
                const auto& fieldVal = dto.*PropertyDescriptor.pointer;
                using FieldType = std::remove_cvref_t<decltype(fieldVal)>;

                if constexpr (is_optional<FieldType>::value) {
                    if (fieldVal.has_value()) {
                        cols.push_back({colName, MakeSQLParam(fieldVal.value())});
                    }
                }
            }
        );

        return cols;
    }

    struct RelationMap {
        std::string TableName;
        std::string TableAlias;
        std::string PrimaryKeyOverride;
        std::string OnCondition;
        std::string JoinType = "LEFT JOIN";
    };

    struct Condition {
        std::string Entity;    // e.g. "Brand" (leave empty for root table fields)
        std::string Field;     // e.g. "Code"
        std::string Operator;  // e.g. "=" or "LIKE"
        std::string Value;     // e.g. "'A'" or "1" (should be sanitized/parameterized by caller)
    };

    struct QueryParts {
        std::string SelectClause;
        std::string JoinClause;
        std::string WhereClause;
    };

    inline QueryParts BuildQueryParts(const std::vector<std::string>& fields, 
                                       const std::vector<Condition>& conditions = {},
                                       const std::map<std::string, RelationMap>& relations = {},
                                       const std::string& rootTableAlias = "")
    {
        QueryParts parts;
        std::map<std::string, bool> alreadyJoined;

        if (fields.empty()) {
            parts.SelectClause = "*";
        } else {
            parts.SelectClause.reserve(fields.size() * 12);
            for (size_t i = 0; i < fields.size(); ++i)
            {
                if (i > 0) parts.SelectClause += ", ";

                size_t dotPos = fields[i].find('.');
                if (dotPos != std::string::npos) {
                    std::string objName = fields[i].substr(0, dotPos);
                    std::string fieldName = fields[i].substr(dotPos + 1);

                    std::string tableAlias = objName;
                    std::string colName = fieldName;

                    if (relations.count(objName)) {
                        const auto& rel = relations.at(objName);
                        if (!rel.TableAlias.empty()) tableAlias = rel.TableAlias;
                        if (fieldName == "Entry" && !rel.PrimaryKeyOverride.empty()) {
                            colName = rel.PrimaryKeyOverride;
                        }

                        if (!alreadyJoined[objName] && !rel.TableName.empty() && !rel.OnCondition.empty()) {
                            std::string joinType = rel.JoinType.empty() ? "LEFT JOIN" : rel.JoinType;
                            parts.JoinClause += " " + joinType + " " + rel.TableName;
                            if (tableAlias != rel.TableName) {
                                parts.JoinClause += " AS " + tableAlias;
                            }
                            parts.JoinClause += " ON " + rel.OnCondition;
                            alreadyJoined[objName] = true;
                        }
                    }

                    if (!colName.empty() && colName.front() != '"' && colName.find('(') == std::string::npos && colName.find(' ') == std::string::npos) {
                        colName = "\"" + colName + "\"";
                    }

                    parts.SelectClause += tableAlias + "." + colName + " AS " + objName + "_" + fieldName;
                } else {
                    std::string f = fields[i];
                    if (!f.empty() && f.front() != '"' && f.find('(') == std::string::npos && f.find(' ') == std::string::npos && f != "*") {
                        f = "\"" + f + "\"";
                    }
                    if (!rootTableAlias.empty()) {
                        parts.SelectClause += rootTableAlias + "." + f;
                    } else {
                        parts.SelectClause += f;
                    }
                }
            }
        }

        for (size_t i = 0; i < conditions.size(); ++i)
        {
            if (i > 0) parts.WhereClause += " AND ";
            
            if (conditions[i].Entity.empty()) {
                if (!rootTableAlias.empty()) {
                    parts.WhereClause += rootTableAlias + "." + conditions[i].Field + " " + conditions[i].Operator + " " + conditions[i].Value;
                } else {
                    parts.WhereClause += conditions[i].Field + " " + conditions[i].Operator + " " + conditions[i].Value;
                }
            } else {
                std::string objName = conditions[i].Entity;
                std::string tableAlias = objName;
                std::string colName = conditions[i].Field;

                if (relations.count(objName)) {
                    const auto& rel = relations.at(objName);
                    if (!rel.TableAlias.empty()) tableAlias = rel.TableAlias;
                    if (colName == "Entry" && !rel.PrimaryKeyOverride.empty()) {
                        colName = rel.PrimaryKeyOverride;
                    }

                    // Add JOIN if not already joined by the Select fields!
                    if (!alreadyJoined[objName] && !rel.TableName.empty() && !rel.OnCondition.empty()) {
                        std::string joinType = rel.JoinType.empty() ? "LEFT JOIN" : rel.JoinType;
                        parts.JoinClause += " " + joinType + " " + rel.TableName;
                        if (tableAlias != rel.TableName) {
                            parts.JoinClause += " AS " + tableAlias;
                        }
                        parts.JoinClause += " ON " + rel.OnCondition;
                        alreadyJoined[objName] = true;
                    }
                }
                
                parts.WhereClause += tableAlias + "." + colName + " " + conditions[i].Operator + " " + conditions[i].Value;
            }
        }

        return parts;
    }

    inline std::string BuildSelectClause(const std::vector<std::string>& fields, const std::map<std::string, RelationMap>& relations = {}, const std::string& rootTableAlias = "")
    {
        if (fields.empty())
            return "*";

        std::string clause;
        clause.reserve(fields.size() * 12); // rough pre-allocation

        for (size_t i = 0; i < fields.size(); ++i)
        {
            if (i > 0) clause += ", ";
            
            size_t dotPos = fields[i].find('.');
            if (dotPos != std::string::npos) {
                std::string objName = fields[i].substr(0, dotPos);
                std::string fieldName = fields[i].substr(dotPos + 1);
                
                std::string tableAlias = objName;
                std::string colName = fieldName;

                if (relations.count(objName)) {
                    tableAlias = relations.at(objName).TableAlias;
                    if (fieldName == "Entry" && !relations.at(objName).PrimaryKeyOverride.empty()) {
                        colName = relations.at(objName).PrimaryKeyOverride;
                    }
                }
                
                clause += tableAlias + "." + colName + " AS " + objName + "_" + fieldName;
            } else {
                // If it's a root field, just use the field name. Caller should handle ambiguity.
                if (!rootTableAlias.empty()) {
                    clause += rootTableAlias + "." + fields[i];
                } else {
                    clause += fields[i];
                }
            }
        }

        return clause;
    }

    inline std::string BuildInsertQuery(const std::string& tableName, const std::vector<std::string>& columns)
    {
        if (columns.empty())
            return "";

        std::string sql = "INSERT INTO " + tableName + " (";
        std::string placeholders;
        sql.reserve(tableName.size() + columns.size() * 15);
        placeholders.reserve(columns.size() * 3);

        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (i > 0)
            {
                sql += ", ";
                placeholders += ", ";
            }
            sql += columns[i];
            placeholders += "?";
        }

        sql += ") VALUES (" + placeholders + ")";
        return sql;
    }

    struct InsertQueryResult {
        std::string Query;
        std::vector<SQLParam> Parameters;
    };

    template <class DTO>
    inline InsertQueryResult BuildInsertQuery(const std::string& tableName, int entry, const DTO& dto)
    {
        InsertQueryResult result;
        std::vector<std::string> cols = {"Entry"};
        result.Parameters.push_back(MakeSQLParam(entry));

        boost::mp11::mp_for_each<boost::describe::describe_members<DTO, boost::describe::mod_public>>(
            [&](auto PropertyDescriptor) {
                std::string colName = PropertyDescriptor.name;
                const auto& fieldVal = dto.*PropertyDescriptor.pointer;
                using FieldType = std::remove_cvref_t<decltype(fieldVal)>;

                if constexpr (is_optional<FieldType>::value) {
                    if (fieldVal.has_value()) {
                        cols.push_back(colName);
                        result.Parameters.push_back(MakeSQLParam(fieldVal.value()));
                    }
                } else {
                    cols.push_back(colName);
                    result.Parameters.push_back(MakeSQLParam(fieldVal));
                }
            }
        );

        result.Query = BuildInsertQuery(tableName, cols);
        return result;
    }

    inline std::string BuildUpdateQuery(const std::string& tableName, const std::vector<std::string>& setColumns, const std::string& whereClause)
    {
        if (setColumns.empty())
            return "";

        std::string sql = "UPDATE " + tableName + " SET ";
        sql.reserve(tableName.size() + setColumns.size() * 15 + whereClause.size() + 10);

        for (size_t i = 0; i < setColumns.size(); ++i)
        {
            if (i > 0) sql += ", ";
            sql += setColumns[i] + " = ?";
        }

        if (!whereClause.empty())
        {
            sql += " WHERE " + whereClause;
        }

        return sql;
    }

    inline UpdateQueryResult BuildUpdateQuery(const std::string& tableName,
                                               const std::vector<ColumnValue>& columns,
                                               const std::string& whereField,
                                               const SQLParam& whereValue)
    {
        UpdateQueryResult result;
        std::vector<std::string> setCols;
        setCols.reserve(columns.size());
        result.Parameters.reserve(columns.size() + 1);

        for (const auto& col : columns)
        {
            setCols.push_back(col.Column);
            result.Parameters.push_back(col.Value);
        }

        std::string formattedWhere = whereField;
        if (!formattedWhere.empty() && formattedWhere.front() != '"' && formattedWhere.front() != '[')
        {
            formattedWhere = "\"" + formattedWhere + "\"";
        }
        result.Query = BuildUpdateQuery(tableName, setCols, formattedWhere + " = ?");
        result.Parameters.push_back(whereValue);

        return result;
    }

} // namespace omnisphere::types
