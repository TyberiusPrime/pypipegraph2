

def escape_logging(s):
    return str(s).replace("<", "\\<").replace("{","{{").replace("}","}}")
