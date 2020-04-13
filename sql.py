import mysql.connector as mysql
from flask import jsonify
import traceback

class Client:
    def __init__(self):
        self.driver = mysql.connect(
            host = "***",
            user = "***",
            passwd = "***",
            db="***"
        )
        self.cursor = self.driver.cursor(buffered=True)

    def validate_key(self, key, country):
        statement = "SELECT country FROM api_publishers WHERE `api_key` = '%s' AND `country` = '%s';" % (key, country)
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount > 0:
            return True
        return False

    def country_exists(self, country):
        statement = "SELECT country FROM country WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            return True
        return False

    def get_all_countries(self):
        final = []
        self.cursor.execute("SELECT country FROM country")
        for row in self.cursor.fetchall():
            final.append(str(row[0]))
        return final

    def get_country_info(self, country):
        final = {}
        statement = "SELECT * FROM country WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount == 0:
            return jsonify ({"error": "No such country '%s' in database" % country}), 400
        row = self.cursor.fetchall()[0]
        final = {
            "country": str(row[0]),
            "description": str(row[1]),
            "riskLevel": str(row[2])
        }
        return final, 200


    def get_embassy(self, country):
        final = {}
        statement = "SELECT address,phone FROM embassy WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "address": str(row[0]),
                "phone": str(row[1])
            }
            return final
        return None

    def get_assistance(self, country):
        final = {}
        statement = "SELECT emergencyPhone FROM assistance WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "emergencyPhone": str(row[0])
            }
            return final
        return None

    def get_safety_and_security(self, country):
        final = {}
        statement = "SELECT crimeRate,flightSafety,roadSafety FROM safety_and_security WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "crimeRate": str(row[0]),
                "flightSafety": str(row[1]),
                "roadSafety": str(row[2])
            }
            return final
        return None

    def get_laws_and_culture(self, country):
        final = {}
        statement = "SELECT laws,culture FROM laws_and_culture WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "laws": str(row[0]),
                "culture": str(row[1])
            }
            return final
        return None

    def get_disaster_and_climate(self, country):
        final = {}
        statement = "SELECT disaster,climate FROM disaster_and_climate WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "disaster": str(row[0]),
                "climate": str(row[1])
            }
            return final
        return None

    def get_entry_and_exit_reqs(self, country):
        final = {}
        statement = "SELECT requirements FROM entry_and_exit WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "requirements": str(row[0]),
            }
            return final
        return None

    def get_health(self, country):
        final = {}
        statement = "SELECT diseases,vaccines FROM health WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "diseases": str(row[0]),
                "vaccines": str(row[1]),
            }
            return final
        return None

    def get_tourism(self, country):
        final = {}
        statement = "SELECT placesOfInterest,topActivites,industryDescription FROM tourism WHERE `country` = '%s';" % country
        self.cursor.execute(statement)
        self.driver.commit()
        if self.cursor.rowcount != 0:
            row = self.cursor.fetchall()[0]
            return {
                "placesOfInterest": str(row[0]),
                "topActivites": str(row[1]),
                "industryDescription": str(row[2]),
            }
            return final
        return None

    def get_advisory(self, country, category):
        # todo: deal with null values
        if not self.country_exists(country):
            return jsonify ({"error": "No such country '%s' in database" % country}), 400
        valid = ['embassy', 'assistance', 'safetyandsecurity', 'lawsandculture', 'naturaldisasterandclimate', 'entryandexit', 'health', 'tourism']
        if category == 'embassy':
            return jsonify({"embassy": self.get_embassy(country)})
        elif category == 'assistance':
            return jsonify({"assistance": self.get_assistance(country)})
        elif category == 'safetyandsecurity':
            return jsonify({"safetyAndSecurity": self.get_safety_and_security(country)})
        elif category == 'lawsandculture':
            return jsonify({"lawsAndCulture": self.get_laws_and_culture(country)})
        elif category == 'naturaldisasterandclimate':
            return jsonify({"naturalDisasterAndCliamte": self.get_disaster_and_climate(country)})
        elif category == 'entryandexit':
            return jsonify({"entryAndExit": self.get_entry_and_exit_reqs(country)})
        elif category == 'health':
            return jsonify({"health": self.get_health(country)})
        elif category == 'tourism':
            return jsonify({"tourism": self.get_tourism(country)})
        else:
            return jsonify({"error": "Invalid advisory category. Must be one of: %s." % (', '.join(valid))}), 400

    def get_all_advisories(self, country):
        if not self.country_exists(country):
            return jsonify ({"error": "No such country '%s' in database" % country}), 400
        final = {}
        embassy = self.get_embassy(country)
        if embassy:
            final["embassy"] = embassy
        emergencyPhone = self.get_assistance(country)
        if emergencyPhone:
            final["assistance"] = emergencyPhone
        safetyAndSecurity = self.get_safety_and_security(country)
        if safetyAndSecurity:
            final["safetyAndSecurity"] = safetyAndSecurity
        lawsAndCulture = self.get_laws_and_culture(country)
        if lawsAndCulture:
            final["lawsAndCulture"] = lawsAndCulture
        disasterAndCliamte = self.get_disaster_and_climate(country)
        if disasterAndCliamte:
            final["naturalDisasterAndCliamte"] = disasterAndCliamte
        entryAndExit = self.get_entry_and_exit_reqs(country)
        if entryAndExit:
            final["entryAndExit"] = entryAndExit
        health = self.get_health(country)
        if health:
            final["health"] = health
        tourism = self.get_tourism(country)
        if tourism:
            final["tourism"] = tourism
        return jsonify(final), 200

    def update_country_info(self, country, description, riskLevel):
        final = {}
        if not description:
            return jsonify ({"error": "No such country '%s' in database" % country}), 400
        statement = "UPDATE `country` SET `description` = '%s'" % description
        if riskLevel:
            if riskLevel in ['Low', 'Medium', 'High']:
                statement += ", `risk_level` = '%s'" % riskLevel
            else:
                return jsonify({"error": "Invalid risk level. Must be one of: Low, Medium, High."}), 400
        statement += " WHERE `country` = '%s';" % country;
        self.cursor.execute(statement)
        self.driver.commit()
        print(statement)
        if self.cursor.rowcount == 0:
            return jsonify({"error": "Nothing to update."}), 400
        return jsonify({"success": True}), 200

    def update_advisory(self, country, category, valuesDict):

        valid = ['embassy', 'assistance', 'safetyandsecurity', 'lawsandculture', 'naturaldisasterandclimate', 'entryandexit', 'health', 'tourism']
        if category not in valid:
            return jsonify({"error": "Invalid advisory category. Must be one of: %s." % (', '.join(valid))}), 400

        statement = None

        if category == 'embassy':
            if set(valuesDict.keys()).issubset(['address', 'phone']):
                statement = self.build_query(country, "embassy", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'assistance':
            if set(valuesDict.keys()).issubset(['emergencyPhone']):
                statement = self.build_query(country, "assistance", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'safetyandsecurity':
            if set(valuesDict.keys()).issubset(['crimeRate', 'flightSafety', 'roadSafety']):
                for key in valuesDict:
                    if valuesDict[key] not in ['Low', 'Medium', 'High']:
                        return jsonify({"error": "Invalid risk level. Must be one of: Low, Medium, High."}), 400
                statement = self.build_query(country, "safety_and_security", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'lawsandculture':
            if set(valuesDict.keys()).issubset(['laws', 'culture']):
                statement = self.build_query(country, "laws_and_culture", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'naturaldisasterandclimate':
            if set(valuesDict.keys()).issubset(['disaster', 'climate']):
                statement = self.build_query(country, "disaster_and_climate", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'entryandexit':
            if set(valuesDict.keys()).issubset(['requirements']):
                statement = self.build_query(country, "entry_and_exit", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'health':
            if set(valuesDict.keys()).issubset(['diseases', 'vaccines']):
                statement = self.build_query(country, "health", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'tourism':
            if set(valuesDict.keys()).issubset(['placesOfInterest', 'topActivites', 'industryDescription']):
                statement = self.build_query(country, "tourism", valuesDict)
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        self.cursor.execute(statement)
        self.driver.commit()
        print(statement)
        if self.cursor.rowcount == 0:
            return jsonify({"error": "Nothing to update."}), 400
        return jsonify({"success": True}), 200

    def build_query(self, country, category, valuesDict):
        statement = "UPDATE `%s` SET " % category
        argCount = 1
        for key in valuesDict:
            statement += "`%s`='%s'" % (key, valuesDict[key])
            if argCount < len(valuesDict):
                statement += ","
            argCount += 1
        statement += " WHERE `country` = '%s';" % country;
        return statement

    def add_new_country(self, country):
        if not country:
            return jsonify({"error": "No country specified in request"}), 400
        try:
            statement = "INSERT INTO `country` (`country`, `description`, `risk level`) VALUES ('%s', 'Default description', 'Low');" % country
            self.cursor.execute(statement)
            self.driver.commit()
            return jsonify({
                "success": True,
                "country": country
            }), 200
        except mysql.errors.IntegrityError:
            self.driver.commit()
            return jsonify({"error": "Duplicate country '%s' in database" % country}), 400
        else:
            self.driver.commit()
            return jsonify({"error": "Failed to insert country '%s'" % country}), 500

    def delete_country(self, country):
        if not country:
            return jsonify({"error": "No country specified in request"}), 400
        try:
            statement = "DELETE FROM `country` WHERE `country`.`country` = '%s';" % country
            self.cursor.execute(statement)
            self.driver.commit()
            if self.cursor.rowcount > 0:
                return jsonify ({
                    "success": True,
                    "deletedCountry": country
                }), 200
            else:
                return jsonify ({"error": "No such country '%s' in database" % country}), 400
        except Exception as e:
            self.driver.commit()
            return jsonify({"error": "Failed to delete country '%s'" % country}), 500

    def close(self):
        self.driver.close()


if __name__ == "__main__":
    client = Client()
    print(client.update_advisory('Canada', 'embassy', {"address": "1232341", "phone": "911"}))
