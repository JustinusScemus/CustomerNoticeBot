class Company:
    def __init__(self, routeslist:list, sort_criteria, colour, displayname, filename, link):
        self.routeslist = routeslist
        self.sort_criteria = sort_criteria
        self.colour = colour
        self.displayname = displayname
        self.filename = filename
        self.link = link

    def circles(self, number):
        return f':{self.colour}_circles:' * number
    
    def squares(self, number):
        return f':{self.colour}_squares:' * number