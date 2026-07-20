#pragma once
#include <string>
#include <vector>
#include <map>

namespace omnisphere::types
{
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
                                      const std::map<std::string, RelationMap>& relations = {})
    {        QueryParts parts;
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

                    parts.SelectClause += tableAlias + ".[" + colName + "] AS " + objName + "_" + fieldName;
                } else {
                    parts.SelectClause += "[" + fields[i] + "]";
                }
            }
        }

        for (size_t i = 0; i < conditions.size(); ++i)
        {
            if (i > 0) parts.WhereClause += " AND ";
            
            if (conditions[i].Entity.empty()) {
                parts.WhereClause += "[" + conditions[i].Field + "] " + conditions[i].Operator + " " + conditions[i].Value;
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
                
                parts.WhereClause += tableAlias + ".[" + colName + "] " + conditions[i].Operator + " " + conditions[i].Value;
            }
        }

        return parts;
    }

    inline std::string BuildSelectClause(const std::vector<std::string>& fields, const std::map<std::string, RelationMap>& relations = {})
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
                
                clause += tableAlias + ".[" + colName + "] AS " + objName + "_" + fieldName;
            } else {
                // If it's a root field, just use the field name. Caller should handle ambiguity.
                clause += "[" + fields[i] + "]";
            }
        }

        return clause;
    }

} // namespace omnisphere::types
