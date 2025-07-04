 
data_s = [
    [
        { 'ms': 1751241300000, 'Value': 93.5, 'sensor': 'humid'
        },
        {'ms': 1751241000000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240700000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240400000, 'Value': 93.5, 'sensor': 'humid'
        }
    ],
    [{'ms': 1751241300000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751241000000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240700000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240400000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240100000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751239800000, 'Value': 93.5, 'sensor': 'humid'
        }],
    [{ 'ms': 1751241300000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751241000000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240700000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240400000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751240100000, 'Value': 93.5, 'sensor': 'humid'
        },
        { 'ms': 1751239800000, 'Value': 93.5, 'sensor': 'humid'
        }]
]

def convert_to_thingboard_format(data_1):
    array_payload = []
    for data_2 in data_1:
        for idx_2,i in enumerate(data_2):
            print(i, idx_2)
            payload = {
                "ts": i['ms'],
                "values":{
                    "temp": float(i['Value']),
                    "humid": float(data_1[1][idx_2]['Value']),
                    "light": float(data_1[2][idx_2]['Value'])
                }
            }
            array_payload.append(payload)
        break
    # print(array_payload)
    return array_payload

ss = convert_to_thingboard_format(data_s)


