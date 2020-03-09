import urllib.request

def calc_demographics():
    demos = urllib.request.urlopen("https://us-east1-dog-project-234515.cloudfunctions.net/get_elo_ranking").read().decode('utf-8')
    print(type(demos))
    return None