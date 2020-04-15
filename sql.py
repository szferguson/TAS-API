import mysql.connector as mysql
from flask import jsonify
import traceback
import time

class Client:
    def __init__(self):
        self.driver = mysql.connect(
            host = "***",
            user = "***",
            passwd = "***",
            db="***"
        )
        self.cursor = self.driver.cursor(buffered=True)

    def update_last_updated(self, country):
        updateTime = int(time.time())
        statement = "CALL updateLastUpdated(%s, %s);"
        self.cursor.execute(statement, (country, updateTime))
        self.driver.commit()

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

    def increment_request_counter(self, client_ip):
        statement = "INSERT INTO requests (client_ip, requests) VALUES('%s', 1) ON DUPLICATE KEY UPDATE client_ip = '%s', requests = requests + 1;" % (client_ip, client_ip)
        self.cursor.execute(statement)
        self.driver.commit()

    def get_all_countries(self):
        final = []
        self.cursor.execute("SELECT country FROM country;") # no need for prepared statement
        self.driver.commit()
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
        self.cursor.execute("SELECT * FROM `last_updated` WHERE `country` = '%s'" % country)
        self.driver.commit()
        if self.cursor.rowcount > 0:
            final["lastUpdated"] = int(self.cursor.fetchall()[0][1])
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
        if not description or not riskLevel:
            return jsonify ({"error": "Please specify a description and riskLevel."}), 400
        if riskLevel not in ['Low', 'Medium', 'High']:
            return jsonify({"error": "Invalid risk level. Must be one of: Low, Medium, High."}), 400
        statement = "CALL updateCountryInfo(%s, %s, %s);"
        values = (country, description, riskLevel)
        self.cursor.execute(statement, values)
        self.driver.commit()
        if self.cursor.rowcount == 0:
            return jsonify({"error": "Nothing to update."}), 400
        self.update_last_updated(country)
        return jsonify({"success": True}), 200

    def update_advisory(self, country, category, valuesDict):

        valid = ['embassy', 'assistance', 'safetyandsecurity', 'lawsandculture', 'naturaldisasterandclimate', 'entryandexit', 'health', 'tourism']
        if category not in valid:
            return jsonify({"error": "Invalid advisory category. Must be one of: %s." % (', '.join(valid))}), 400

        statement = None
        values = None

        if category == 'embassy':
            if set(valuesDict.keys()) == set(['address', 'phone']):
                statement = "CALL updateEmbassy(%s, %s, %s);"
                values = (country, valuesDict['address'], valuesDict['phone'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'assistance':
            if set(valuesDict.keys()) == set(['emergencyPhone']):
                statement = "CALL updateAssistance(%s, %s);"
                values = (country, valuesDict['emergencyPhone'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'safetyandsecurity':
            if set(valuesDict.keys()) == set(['crimeRate', 'flightSafety', 'roadSafety']):
                for key in valuesDict:
                    if valuesDict[key] not in ['Low', 'Medium', 'High']:
                        return jsonify({"error": "Invalid risk level. Must be one of: Low, Medium, High."}), 400
                statement = "CALL updateSafetyAndSecurity(%s, %s, %s, %s);"
                values = (country, valuesDict['crimeRate'], valuesDict['flightSafety'], valuesDict['roadSafety'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'lawsandculture':
            if set(valuesDict.keys()) == set(['laws', 'culture']):
                statement = "CALL updateLawsAndCulture(%s, %s, %s);"
                values = (country, valuesDict['laws'], valuesDict['culture'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'naturaldisasterandclimate':
            if set(valuesDict.keys()) == set(['disaster', 'climate']):
                statement = "CALL updateClimate(%s, %s, %s);"
                values = (country, valuesDict['disaster'], valuesDict['climate'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'entryandexit':
            if set(valuesDict.keys()) == set(['requirements']):
                statement = "CALL updateRequirements(%s, %s);"
                values = (country, valuesDict['requirements'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'health':
            if set(valuesDict.keys()) == set(['diseases', 'vaccines']):
                statement = "CALL updateHealth(%s, %s, %s);"
                values = (country, valuesDict['diseases'], valuesDict['vaccines'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        elif category == 'tourism':
            if set(valuesDict.keys()) == set(['placesOfInterest', 'topActivites', 'industryDescription']):
                statement = "CALL updateTourism(%s, %s, %s, %s);"
                values = (country, valuesDict['placesOfInterest'], valuesDict['topActivites'], valuesDict['industryDescription'])
            else:
                return jsonify({"error": "Invalid keys for category: %s" % category}), 400

        self.cursor.execute(statement, values)
        self.driver.commit()

        if self.cursor.rowcount == 0:
            return jsonify({"error": "Nothing to update."}), 400
        self.update_last_updated(country)
        return jsonify({"success": True}), 200

    def add_new_country(self, country):
        if not country:
            return jsonify({"error": "No country specified in request"}), 400
        try:
            updateTime = int(time.time())
            statement = "CALL createCountry(%s, %s);"
            self.cursor.execute(statement, (country, updateTime))
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
            statement = "CALL deleteCountry('%s');" % country
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
