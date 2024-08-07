class Company:
    def __init__(self, routeslist:list, sort_criteria, colour, displayname, filename, link):
        self.routeslist = routeslist
        self.sort_criteria = sort_criteria
        self.colour = colour
        self.displayname = displayname
        self.filename = filename
        self.link = link
        self.removed_buffer = set()

    def circles(self, number):
        return f':{self.colour}_circle:' * number
    
    def squares(self, number):
        return f':{self.colour}_square:' * number