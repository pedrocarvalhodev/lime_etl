#!/usr/bin/python
# -*- coding: utf-8 -*-

from base64 import b64decode
import json
import sys
import requests
from time import sleep


class Api:
    def __init__(self, url, user, key):
        self.url = url
        self._user = user
        self._password = key

        data = """{   "id": 1,
                    "method": "get_session_key",
                    "params": { "username": "%s",
                                "password": "%s" } } """ % (user, key)
        self.session_key = self._getJSON(data)['result']

    # Standard post request
    def _getJSON(self, data):
        headers = {'content-type': 'application/json',
                   'connection': 'Keep-Alive'}
        try:
            req = requests.post(self.url, data=data, headers=headers)
            return(req.json())
        except:
            e = sys.exc_info()[0]
            print ("<p>Error: %s</p>" % e)

    def delete_survey(self, sid):
        data = """{ "id": 1,
                    "method": "delete_survey",
                    "params": { "sSessionKey": "%s",
                                "iSurveyID": %s } }""" % (self.session_key,
                                                          sid)
        return self._getJSON(data)['result']

    def set_survey_property(self, sid, prop, value):
        data = """{ "id": 1,
                    "method": "set_survey_properties",
                    "params": { "sSessionKey": "%s",
                                "iSurveyID": %s,
                                "aSurveySettings": { "%s": "%s" }
            } }""" % (self.session_key, sid, prop, value)
        return self._getJSON(data)['result']

    def get_survey_properties(self, sid, settings=None):

        if settings is None:
            settings = """ [
            "sid","savetimings","allowprev","tokenanswerspersistence",
            "showgroupinfo","showwelcome","owner_id","template","printanswers",
            "assessments","shownoanswer","showprogress","admin","language",
            "ipaddr","usecaptcha","showqnumcode","allowjumps","active",
            "additional_languages","refurl","usetokens","bouncetime",
            "navigationdelay","expires","datestamp","datecreated",
            "bounce_email","bounceprocessing","nokeyboard","startdate",
            "usecookie","publicstatistics","attributedescriptions",
            "bounceaccounttype","alloweditaftercompletion","adminemail",
            "allowregister","publicgraphs","emailresponseto",
            "bounceaccounthost","googleanalyticsstyle","anonymized",
            "allowsave","listpublic","emailnotificationto","bounceaccountpass",
            "googleanalyticsapikey","faxto","autonumber_start","htmlemail",
            "tokenlength","bounceaccountencryption","format","autoredirect",
            "sendconfirmation","showxquestions","bounceaccountuser" ] """

        data = """{ "id": 1,
                    "method": "get_survey_properties",
                    "params": { "sSessionKey": "%s",
                                "iSurveyID": %s,
                                "aSurveySettings": %s
            } }""" % (self.session_key, sid, settings)
        return self._getJSON(data)['result']

    def get_summary(self, sid):
        data = """{ "id": 1,
                    "method": "get_summary",
                    "params": { "sSessionKey": "%s",
                                "iSurveyID": %s,
                                "sStatname": "all" } }""" % (self.session_key,
                                                             sid)
        return self._getJSON(data)['result']

    def activate_survey(self, sid):
        data = """{ "id": 1,
                    "method": "activate_survey",
                    "params": { "sSessionKey": "%s",
                                "SurveyID": %s } }""" % (self.session_key, sid)
        return self._getJSON(data)['result']

    def import_survey(self, idata, title, sid, type='lss'):
        data = """{ "id": 1,
                    "method": "import_survey",
                    "params": { "sSessionKey": "%s",
                                "sImportData": "%s",
                                "sImportDataType": "%s",
                                "sNewSurveyName": "%s",
                                "DestSurveyID": %d } }""" \
               % (self.session_key, idata, type, title, sid)
        return self._getJSON(data)['result']

    def release_session_key(self):
        data = """ { "method": "release_session_key",
                     "params": { "sSessionKey" : "%s"},
                     "id":1}' }""" % (self.session_key)
        return self._getJSON(data)['result']

    def export_responses(self, sid, status='all', heading='code', response='short', fields=''):
        data = """ {    "id" : 1,
                        "method":"export_responses",
                        "params": { "sSessionKey": "%s",
                                    "iSurveyID":  %s,
                                    "sDocumentType": "json",
                                    "sLanguageCode": "",
                                    "sCompletionStatus": "%s",
                                    "sHeadingType": "%s",
                                    "sResponseType": "%s",
                                    "aFields": "%s"
                        } } """ % (self.session_key, sid, status, heading, response, fields)
        print(data)
        out = b64decode(self._getJSON(data)['result']).decode('utf-8')
        return json.loads(out)

    def export_responses_by_token(self, sid, token):
        data = """ {    "id" : 1,
                        "method":"export_responses_by_token",
                        "params": { "sSessionKey": "%s",
                                    "iSurveyID":  %s,
                                    "sDocumentType": "json",
                                    "sToken":  "%s",
                                    "$sLanguageCode": "en",
                                    "sCompletationStatus": "all",
                                    "sHeadingType": "code",
                                    "sResponseType": "short"
                        } } """ % (self.session_key, sid, token)
        return self._getJSON(data)['result']

    def _add_response(self, sid, rdata):
        data = """ {          "id": 1,
                              "method":"add_response",
                              "params": { "sSessionKey": "%s",
                                          "iSurveyID": %s,
                                          "aResponseData": %s }
                    } """ % (self.session_key, sid, rdata)
        return self._getJSON(data)['result']

    def importar_desde_archivo(self, sid, archivo):
        """Esto no funciona!"""

        with open(archivo) as csv:
            datos = []
            for linea in csv.readlines():
                datos.append(linea.rstrip().split('\t'))

        columnas = datos[1]
        for d in datos[2:]:
            r = dict(zip(columnas, d))
            r['id'] = ""
            self._add_response(sid, json.dumps(r))
            sleep(1)

    def _list_groups(self, sid):
        data = """ {          "method":"list_groups",
                              "params": { "sSessionKey": "%s",
                                          "iSurveyID": %s },
                            "id": 1 } """ % (self.session_key, sid)
        return self._getJSON(data)['result']

    def list_groups(self, sid):
        json_list_groups = self._list_groups(sid)

        groups = []
        for g in json_list_groups:
            group = g['id']['gid'], g['group_name']
            groups.append(group)

        return groups

    def _list_questions(self, sid, gid):
        data = """ {          "method":"list_questions",
                              "params": { "sSessionKey": "%s",
                                          "iSurveyID": %s,
                                          "iGroupID": %s },
                            "id": 1 } """ % (self.session_key, sid, gid)
        return self._getJSON(data)['result']

    def list_questions(self, sid, gid):
        json_list_questions = self._list_questions(sid, gid)

        questions = []
        for q in json_list_questions:
            question = q['id']['qid'], q['question']
            questions.append(question)
        return questions

    def list_surveys(self, sUser=''):
        data = """ { "id" : 1,
                     "method":"list_surveys",
                     "params": { "sSessionKey": "%s", "sUsername": "%s"} 
                     } """ % (self.session_key, sUser)
        return self._getJSON(data)['result']

    def list_participants(self, sid, iStart=0, iLimit=1000000, bUnused='true', aAttributes='true', aConditions='array()'):
        data = """ {    "id" : 1,
                        "method":"list_participants",
                        "params": { "sSessionKey": "%s",
                                    "iSurveyID":  %s,
                                    "iStart": %s,
                                    "iLimit": %s,
                                    "bUnused": "%s",
                                    "aAttributes": "%s",
                                    "aConditions": "%s"
                        } } """ % (self.session_key, sid, iStart, iLimit, bUnused, aAttributes, aConditions)
        return self._getJSON(data)['result']
