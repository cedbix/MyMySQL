#ifndef CEDBIX_MYMYSQL_H
#define CEDBIX_MYMYSQL_H

#include <mysql.h>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <iostream>
#include <iomanip>

namespace cedbix {

	//class MySQLException : public std::exception {};

	static const std::string APOSTROPHE = "`";
	static const std::string APOSTROPHE2 = "'";
	static const std::string COMMA = ",";
	static const std::string CREATE_TABLE = "CREATE TABLE `";
	static const std::string BR1 = "(";
	static const std::string BR2 = ")";
	static const std::string SPACE = " ";
	static const std::string COMMENT = "COMMENT='";
	static const std::string FOREIGN_KEY = "FOREIGN KEY (`";
	static const std::string REFERENCES = "REFERENCES `";
	static const std::string BR_AP = "(`";
	static const std::string AP_BR = "`)";

	struct MyField
	{
		MyField() { dim = 0; };
		MyField(std::string line)
		{
			//std::cout << line << std::endl;
			allowNull = true;
			autoIncrement = false;
			defaultValue = "";
			name = "";
			type = "";
			dim = 0;
			ref = "";
			refCol = "";
			size_t p1, p2;
			std::stringstream ss(line);
			ss >> name >> type;
			p1 = name.find(APOSTROPHE);
			p2 = name.find(APOSTROPHE, p1 + 1);
			if (p1 != std::string::npos && p2 != std::string::npos)
				name = name.substr(p1 + 1, p2 - p1 - 1);

			p1 = type.find(BR1);
			p2 = type.find(BR2, p1 + 1);
			if (p1 != std::string::npos && p2 != std::string::npos)
			{
				dim = std::atol(type.substr(p1 + 1, p2 - p1 - 1).c_str());
				type = type.substr(0, p1);
			}

			while (ss)
			{
				std::string str;
				ss >> str;
				//std::cout << str << std::endl;
				if (str == "NOT")
				{
					ss >> str;
					if (str.find("NULL") != std::string::npos)
					{
						allowNull = false;
					}
				}
				else if (str.find("NULL") != std::string::npos)
				{
					allowNull = true;
				}
				else if (str.find("AUTO_INCREMENT") != std::string::npos)
				{
					autoIncrement = true;
				}
				else if (str == "DEFAULT")
				{
					ss >> defaultValue;
					//boost::erase_all(defaultValue, "'");
					//boost::erase_all(defaultValue, ",");
				}
			}
		}

		std::string toString()
		{
			std::stringstream ss;
			ss << "{\"name\": \"" << name << "\", "
				<< "\"type\": \"" << type << "\"";
			if (dim > 0)
				ss << ", \"dim\": " << dim << "";

			ss << ", \"allowNull\": " << std::boolalpha << allowNull << "";
			ss << ", \"autoIncrement\": " << std::boolalpha << autoIncrement << "";
			ss << ", \"defalut\": \"" << defaultValue << "\"";
			if (!(ref.empty() || refCol.empty()))
			{
				ss << ", \"reference\": { "
					<< "\"table\": \"" << ref << "\","
					<< "\"column\": \"" << refCol << "\""
					<< "}";
			}
			ss << "}";
			return ss.str();
		}

		std::string name;
		std::string type;
		std::string ref;
		std::string refCol;
		bool allowNull;
		std::string defaultValue;
		bool autoIncrement;
		int dim;
	};

	struct MyRecords
	{
		MyRecords() { };

		std::vector<std::string> toStrings()
		{
			std::vector<std::string> res;

			for (int i = 0; i < data.size(); ++i)
			{
				std::stringstream ss;
				ss << "{";
				for (int j = 0; j < fields.size(); ++j)
				{
					if (j > 0)
						ss << ", ";
					ss << "\"" << fields.at(j) << "\": \"" << data.at(i).at(j) << "\"";
				}
				ss << "}";
				res.push_back(ss.str());
			}
			return res;
		}

		int fieldNum;
		std::vector<std::string> fields;
		std::vector<std::string> types;
		std::vector<std::vector<std::string>> data;

	};

	class MyTable
	{
	public:
		MyTable(std::string createTable)
		{
			std::stringstream ct(createTable);
			std::string line;

			while (std::getline(ct, line))
				parseLine(line);
		}

		std::string toString()
		{
			std::stringstream ss;
			ss << "\"table\": \"" << name << "\", ";
			if (!key.empty())
				ss << "\"key\": \"" << key << "\", ";
			if (!description.empty())
				ss << "\"description\": \"" << description << "\", ";
			ss << "\"columns\": [";
			std::vector<MyField>::iterator it, end;
			for (it = fields.begin(); it < fields.end(); ++it)
			{
				if (it != fields.begin()) ss << ", ";
				ss << it->toString();
			}
			ss << "]";
			return ss.str();
		}
		std::vector<std::string> getLinkedTables()
		{
			std::vector<std::string> lt;
			for (auto f : fields)
			{
				if (!f.ref.empty())
					lt.push_back(f.ref);
			}
			return lt;
		}
		std::string getKey()
		{
			return key;
		}
		std::vector<std::string> getFieldNames()
		{
			std::vector<std::string> names;
			for (MyField f : fields)
			{
				names.push_back(f.name);
			}
			return names;
		}

		std::vector<MyField> getFields()
		{
			return fields;
		}

	private:
		MyTable() { count = 0; };

		std::string name;
		std::string description;
		std::string key;
		long count;
		std::vector<MyField> fields;

		void parseLine(std::string line)
		{
			std::stringstream ss(line);
			std::string first;
			ss >> first;

			if (first.compare("CREATE") == 0)
			{
				std::string second, third;
				ss >> second >> third;
				size_t ap1 = third.find(APOSTROPHE);
				size_t ap2 = third.find(APOSTROPHE, ap1 + 1);
				if (ap1 != std::string::npos && ap2 != std::string::npos)
					name = third.substr(ap1 + 1, ap2 - ap1 - 1);
			}
			else if (first.front() == '`')
			{
				fields.push_back(MyField(line));
			}
			else if (first.compare("PRIMARY") == 0)
			{
				size_t ap1 = line.find(APOSTROPHE);
				size_t ap2 = line.find(APOSTROPHE, ap1 + 1);
				if (ap1 != std::string::npos && ap2 != std::string::npos)
					key = line.substr(ap1 + 1, ap2 - ap1 - 1);
			}
			else if (first.compare("KEY") == 0)
			{
				//std::cout << "key line: " << line << std::endl;
			}
			else if (first.compare("CONSTRAINT") == 0)
			{
				size_t p1, p2;
				std::string fkey;
				p1 = line.find(FOREIGN_KEY);
				if (p1 != std::string::npos)
				{
					p1 += FOREIGN_KEY.size();
					p2 = line.find('`', p1);
					if (p2 != std::string::npos)
						fkey = line.substr(p1, p2 - p1);
				}

				std::string references;
				p1 = line.find(REFERENCES, p2);
				if (p1 != std::string::npos)
				{
					p1 += REFERENCES.size();
					p2 = line.find('`', p1);
					if (p2 != std::string::npos)
						references = line.substr(p1, p2 - p1);
				}

				std::string reference_col;
				size_t pp1 = line.find(BR_AP, p2) + BR_AP.size();
				if (pp1 != std::string::npos)
				{
					p2 = line.find(AP_BR, pp1);
					if (p2 != std::string::npos)
					{
						reference_col = line.substr(pp1, p2 - pp1);
						std::vector<MyField>::iterator it;
						for (it = fields.begin(); it < fields.end(); ++it)
						{
							if (it->name.compare(fkey) == 0)
							{
								it->ref = references;
								it->refCol = reference_col;
							}
						}
					}
				}
			}
			else if (first.front() == ')')
			{
				size_t p1 = line.find(COMMENT);
				if (p1 != std::string::npos)
				{
					p1 += COMMENT.size();
					size_t p2 = line.find('\'', p1);
					if (p2 != std::string::npos)
						description = line.substr(p1, p2 - p1);
				}
			}
			else
			{
				//std::cout << "unknown  line: " << std::endl;
			}
		}
	};


	class MySQLResult
	{
	public:
		MySQLResult(MYSQL_RES *res) : res(res) {};
		~MySQLResult() { mysql_free_result(res); };
		MYSQL_RES* result() { return res; };
		std::string toJson()
		{
			std::stringstream json;
			unsigned long num = (unsigned long)mysql_num_rows(res);
			if (num == 0) return "";
			std::vector<std::string> fieldNames;
			std::map<std::string, enum_field_types> fieldTypes;
			MYSQL_FIELD *field;
			while (field = mysql_fetch_field(res))
			{
				std::string name = std::string(field->name);
				fieldNames.push_back(name);
				fieldTypes[name] = field->type;
			}
			int counter = 0;
			if (num > 1) json << "[";
			MYSQL_ROW row;
			while (row = mysql_fetch_row(res))
			{
				if (counter++ > 0) json << ", ";
				json << "{";
				bool first = true;
				for (int i = 0; i < fieldNames.size(); ++i)
				{
					if (row[i])
					{
						std::string value(row[i]);
						//boost::replace_all(value, "\"", "\\\"");
						std::string &name = fieldNames.at(i);
						enum_field_types &t = fieldTypes[name];
						if (!first) json << ", "; else first = false;
						json << "\"" << name << "\": ";
						if (!IS_NUM(t)) json << '\"';
						for (char c : value)
						{
							if (c == '\"' && c == '\r' && c == '\n' && c == '\t' && c == '\v') json << "\\";
							json << c;
						}
						if (!IS_NUM(t)) json << '\"';
					}
				}
				json << "}";
			}
			if (num > 1) json << "]";
			return json.str();
		};
	private:
		MYSQL_RES *res;
	};

	class MySQLConnection
	{

	public:

		MySQLConnection(std::string host, std::string database, std::string login, std::string password)
		{
			std::string charset = "utf8";
			connection = mysql_init(NULL);
			mysql_real_connect(connection, host.c_str(), login.c_str(), password.c_str(), database.c_str(), 0, NULL, 0);
			bool b = true;
			my_bool bd = (my_bool)1;
			mysql_options(connection, MYSQL_OPT_RECONNECT, &bd);
			CheckError();
			mysql_set_character_set(connection, charset.c_str());
		};

		~MySQLConnection() { mysql_close(connection); };

		MySQLResult query(std::string query) {
			std::cout << query << std::endl;
			MYSQL_RES *res;
			mysql_query(connection, query.c_str());
			CheckError();
			res = mysql_store_result(connection);
			return MySQLResult(res);
		};

		std::string query0(std::string query)
		{
			std::cout << query << std::endl;
			//std::cout << query << std::endl;
			mysql_query(connection, query.c_str());
			CheckError();
			std::stringstream ss;
			ss << "Rows affected: " << rowAffected();
			return ss.str();
		};

		MySQLResult selectAll(std::string table)
		{
			std::stringstream ss;
			ss << "select * from " << table << " limit 1000;";
			return query(ss.str());
		};

		MySQLResult selectByKey(std::string table, std::string key)
		{
			MyTable tab = tableStructure(table);
			std::string keyField = tab.getKey();
			if (keyField.empty())
			{
				std::stringstream er;
				er << "Table " << table << " has no key field";
				throw std::exception(er.str().c_str());
				//throw HttpBadRequestException(er.str().c_str());
			}
			std::stringstream ss;
			ss << "SELECT * FROM `" << table << "` WHERE `" << keyField << "` = '" << key << "';";
			return query(ss.str());
		};

		MySQLResult selectLastInserted(std::string table)
		{
			MyTable tab = tableStructure(table);
			std::string keyField = tab.getKey();
			std::stringstream sel;
			sel << "SELECT * FROM `" << table << "` WHERE `" << keyField << "`= LAST_INSERT_ID();";
			return query(sel.str());
		};

		MySQLResult updateByKey(std::string table, std::string key, std::map<std::string, std::string> &values)
		{
			MyTable tab = tableStructure(table);
			std::string keyField = tab.getKey();
			if (keyField.empty())
			{
				std::stringstream er;
				er << "Table " << table << " has no key field";
				throw std::exception(er.str().c_str());
				//throw HttpBadRequestException(er.str().c_str());
			}
			std::stringstream ss;
			ss << "UPDATE `" << table << "` SET ";
			int count = 0;
			for (auto f : tab.getFields())
			{
				if (f.name != keyField && !values[f.name].empty())
				{
					char res[sizeof(values[f.name]) * 3 + 1];
					cp1251_to_utf8(res, values[f.name].data());
					std::string value(res);
					if (count++ > 0) ss << ", ";
					ss << "`" << f.name << "`" << " = ";
					if ((f.type == "date" || f.type == "time" || f.type == "datetime") && is_number(value))
						ss << "FROM_UNIXTIME(" << value << ")";
					else if (value == "NULL")
						ss << "NULL";
					else
						ss << "'" << value << "'";
				}
			}

			ss << " WHERE `" << keyField << "` = '" << key << "';";

			query0(ss.str());
			return selectByKey(table, key);
		};

		MySQLResult insert(std::string table, std::map<std::string, std::string> &values)
		{
			MyTable tab = tableStructure(table);
			std::stringstream ins, val, sel;
			std::string keyField = tab.getKey();
			ins << "INSERT INTO `" << table << "` ( ";
			val << "VALUES (";
			int count = 0;

			for (auto f : tab.getFields())
			{
				if (f.name != keyField && !values[f.name].empty())
				{
					char res[sizeof(values[f.name]) * 3 + 1];
					cp1251_to_utf8(res, values[f.name].data());
					std::string value(res);
					if (count++ > 0)
					{
						ins << ", ";
						val << ", ";
					}
					ins << "`" << f.name << "`";

					if ((f.type == "date" || f.type == "time" || f.type == "datetime") && is_number(value))
						val << "FROM_UNIXTIME(" << value << ")";
					else if (value == "NULL")
						val << "NULL";
					else
						val << "'" << value << "'";
				}
			}

			ins << ") ";
			val << ");";
			ins << val.str();
			query0(ins.str());
			sel << "SELECT * FROM `" << table << "` WHERE `" << keyField << "`= LAST_INSERT_ID();";
			return query(sel.str());
		};

		std::string deleteByKey(std::string table, std::string key)
		{
			MyTable tab = tableStructure(table);
			std::string keyField = tab.getKey();
			if (keyField.empty())
			{
				std::stringstream er;
				er << "Table " << table << " has no key field";
				//throw HttpBadRequestException(er.str().c_str());
				throw std::exception(er.str().c_str());
			}
			std::stringstream ss;
			ss << "DELETE FROM `" << table << "` WHERE `" << keyField << "` = '" << key << "';";
			return query0(ss.str());
			//query0(ss.str());
			//return "{\"" + keyField + "\": " + key + "}"
		};

		MyTable tableStructure(std::string table)
		{
			std::stringstream ss;
			ss << "SHOW CREATE TABLE " << table << ";";
			MySQLResult tsRes = query(ss.str());
			MYSQL_ROW tsRow = mysql_fetch_row(tsRes.result());
			return MyTable(tsRow[1]);
		};

		std::vector<MyTable> databaseStructure()
		{
			std::vector<std::string> tables = showTables();
			return databaseStructure(tables);
		};

		std::vector<MyTable> databaseStructure(std::vector<std::string> &tables)
		{
			std::vector<MyTable> structure;
			for (auto &t : tables)
			{
				structure.push_back(tableStructure(t));
			}
			return structure;
		};

		unsigned long rowCount(std::string table)
		{
			std::stringstream ss;
			ss << "SELECT count(*) from " << table << ";";
			MySQLResult tsRes = query(ss.str());
			MYSQL_ROW tsRow = mysql_fetch_row(tsRes.result());
			return std::stoul(tsRow[0]);
		};

		std::vector<std::string> showTables()
		{
			std::vector<std::string> tables;
			MySQLResult res = query("SHOW TABLES;");
			MYSQL_ROW row;
			while (row = mysql_fetch_row(res.result()))
			{
				if (row[0])
					tables.push_back(row[0]);
			}
			return tables;
		};

		unsigned long rowAffected()
		{
			return (unsigned long)mysql_affected_rows(connection);
		};

		std::vector<std::string> linkedTables(std::string table)
		{
			std::set<std::string> tables;
			getlinkedTables(tables, table);
			std::vector<std::string> out(tables.begin(), tables.end());
			out.push_back(table);
			return out;
		};

		void getlinkedTables(std::set<std::string> &tables, std::string table)
		{
			MyTable structure = tableStructure(table);
			std::vector<std::string> lt = structure.getLinkedTables();
			for (std::string t : lt)
			{
				if (tables.count(t) == 0)
				{
					tables.insert(t);
					getlinkedTables(tables, t);
				}
			}
		};

	private:
		MYSQL* connection;
		void CheckError()
		{
			const char* status = mysql_error(connection);
			if (status[0])
			{
				std::stringstream ss;
				ss << "MySQL Exception: " << std::string(status);
				throw std::exception(ss.str().c_str());
			}
		};



		bool is_number(std::string str)
		{
			bool res = true;
			for (char c : str)
				res = isdigit(c);
			return res;
		}

		static void escapeSymbols(std::string &str) {
			static const std::vector<char> eSym = { '\r', '\n', '\t' ,'\v', '\"', '\\' };

		}

		static void cp1251_to_utf8(char *out, const char *in) {
			static const int table[128] = {
				0x82D0,0x83D0,0x9A80E2,0x93D1,0x9E80E2,0xA680E2,0xA080E2,0xA180E2,
				0xAC82E2,0xB080E2,0x89D0,0xB980E2,0x8AD0,0x8CD0,0x8BD0,0x8FD0,
				0x92D1,0x9880E2,0x9980E2,0x9C80E2,0x9D80E2,0xA280E2,0x9380E2,0x9480E2,
				0,0xA284E2,0x99D1,0xBA80E2,0x9AD1,0x9CD1,0x9BD1,0x9FD1,
				0xA0C2,0x8ED0,0x9ED1,0x88D0,0xA4C2,0x90D2,0xA6C2,0xA7C2,
				0x81D0,0xA9C2,0x84D0,0xABC2,0xACC2,0xADC2,0xAEC2,0x87D0,
				0xB0C2,0xB1C2,0x86D0,0x96D1,0x91D2,0xB5C2,0xB6C2,0xB7C2,
				0x91D1,0x9684E2,0x94D1,0xBBC2,0x98D1,0x85D0,0x95D1,0x97D1,
				0x90D0,0x91D0,0x92D0,0x93D0,0x94D0,0x95D0,0x96D0,0x97D0,
				0x98D0,0x99D0,0x9AD0,0x9BD0,0x9CD0,0x9DD0,0x9ED0,0x9FD0,
				0xA0D0,0xA1D0,0xA2D0,0xA3D0,0xA4D0,0xA5D0,0xA6D0,0xA7D0,
				0xA8D0,0xA9D0,0xAAD0,0xABD0,0xACD0,0xADD0,0xAED0,0xAFD0,
				0xB0D0,0xB1D0,0xB2D0,0xB3D0,0xB4D0,0xB5D0,0xB6D0,0xB7D0,
				0xB8D0,0xB9D0,0xBAD0,0xBBD0,0xBCD0,0xBDD0,0xBED0,0xBFD0,
				0x80D1,0x81D1,0x82D1,0x83D1,0x84D1,0x85D1,0x86D1,0x87D1,
				0x88D1,0x89D1,0x8AD1,0x8BD1,0x8CD1,0x8DD1,0x8ED1,0x8FD1
			};
			while (*in)
				if (*in & 0x80) {
					int v = table[(int)(0x7f & *in++)];
					if (!v)
						continue;
					*out++ = (char)v;
					*out++ = (char)(v >> 8);
					if (v >>= 16)
						*out++ = (char)v;
				}
				else
					*out++ = *in++;
			*out = 0;
		}
	};
}
#endif // CEDBIX_MYMYSQL_H