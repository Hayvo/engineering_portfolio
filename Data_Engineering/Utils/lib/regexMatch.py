import regex


def isWebsiteUrlValid(url):
    return regex.match(r"^(https?://)?(www\.)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,6}(/[\w\-./?%&=]*)?$", url) is not None