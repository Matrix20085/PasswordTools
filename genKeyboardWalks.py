from itertools import combinations_with_replacement

columnList = [
    '1qaz',
    '2wsx',
    '3edc',
    '4rfv',
    '5tgb',
    '6yhn',
    '7ujm',
    '8ik,',
    '9ol.',
    '0p;/',
    '!QAZ',
    '@WSX',
    '#EDC',
    '$RFV',
    '%TGB',
    '^YHN',
    '&UJM',
    '*IK<v',
    '(OL>',
    ')P:?'
]

rowList = [
    'qwer',
    'asdf',
    'zxcv',
    '1234',
    '!@#$',
    'QWER',
    'ASDF',
    'ZXCV',
    'qwerty',
    'asdfgh',
    'zxcvbn',
    '123456',
    '!@#$%^',
    'QWERTY',
    'ASDFGH',
    'ZCXVBN',
    'qwertyuiop',
    '*asdfghjkl',
    'zxcvbnm',
    '1234567890'
]

output_file = "keyboardWalkComboRow.txt"

with open(output_file, "w") as f:
    buffer = []
    for r in range(1, 11):
        for combo in combinations_with_replacement(rowList, r):
            buffer.append("".join(combo))
            if len(buffer) >= 1000:
                f.write("\n".join(buffer) + "\n")
                buffer = []
    if buffer:
        f.write("\n".join(buffer) + "\n")

output_file = "keyboardWalkComboColumn.txt"

with open(output_file, "w") as f:
    buffer = []
    for r in range(1, 11):
        for combo in combinations_with_replacement(columnList, r):
            buffer.append("".join(combo))
            if len(buffer) >= 1000:
                f.write("\n".join(buffer) + "\n")
                buffer = []
    if buffer:
        f.write("\n".join(buffer) + "\n")