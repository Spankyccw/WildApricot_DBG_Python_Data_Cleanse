#Title: Python_test_01_from_Data_Annotation.py
#Purpose: A Unicode mapping test based on a grid from Data Annotation
#Change: ccwilliams 20251002 Initial version created with Claude
# Date/Name/Change
import requests
from html.parser import HTMLParser

class GridDataParser(HTMLParser):
    """Parser to extract coordinate and character data from HTML tables."""
    
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.current_row = []
        self.data_rows = []
        self.in_td = False
        
    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
        elif tag == 'td' and self.in_table:
            self.in_td = True
            
    def handle_endtag(self, tag):
        if tag == 'table':
            self.in_table = False
        elif tag == 'tr' and self.in_table:
            if len(self.current_row) == 3:
                self.data_rows.append(self.current_row)
            self.current_row = []
        elif tag == 'td':
            self.in_td = False
            
    def handle_data(self, data):
        if self.in_td:
            stripped = data.strip()
            if stripped:
                self.current_row.append(stripped)

def display_grid_from_url(url):
    """
    Reads a document from the given URL containing Unicode characters and coordinates,
    then displays them as a 2D grid.
    
    Parameters:
        url (str): The URL of the document containing the grid data
        
    The document should contain three columns:
        - x-coordinate: horizontal position (0-based)
        - Character: Unicode character to display
        - y-coordinate: vertical position (0-based)
    """
    
    # Fetch the document
    response = requests.get(url)
    response.raise_for_status()
    
    # Parse the HTML to extract data
    parser = GridDataParser()
    parser.feed(response.text)
    
    # Build a dictionary mapping (x, y) coordinates to characters
    grid_dict = {}
    max_x = 0
    max_y = 0
    
    for row in parser.data_rows:
        # Skip header row
        if row[0] == 'x-coordinate':
            continue
            
        try:
            x = int(row[0])
            char = row[1]
            y = int(row[2])
            
            grid_dict[(x, y)] = char
            max_x = max(max_x, x)
            max_y = max(max_y, y)
        except (ValueError, IndexError):
            # Skip malformed rows
            continue
    
    # Create and print the grid
    # Print from max_y down to 0 (bottom-left origin, y increases upward)
    for y in range(max_y, -1, -1):
        line = []
        for x in range(max_x + 1):
            line.append(grid_dict.get((x, y), ' '))
        print(''.join(line))

# Example usage
if __name__ == "__main__":
    #url1 = "https://docs.google.com/document/d/e/2PACX-1vTMOmshQe8YvaRXi6gEPKKlsC6UpFJSMAk4mQjLm_u1gmHdVVTaeh7nBNFBRlui0sTZ-snGwZM4DBCT/pub"
    url2 = "https://docs.google.com/document/d/e/2PACX-1vRPzbNQcx5UriHSbZ-9vmsTow_R6RRe7eyAU60xIF9Dlz-vaHiHNO2TKgDi7jy4ZpTpNqM7EvEcfr_p/pub"
    display_grid_from_url(url2)