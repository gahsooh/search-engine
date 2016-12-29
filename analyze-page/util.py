# -*- coding: utf-8 -*-

import json

def encode(data):
    data = data.encode('utf-8') if type(data) == unicode else data
    return json.loads(data)