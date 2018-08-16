from api import app
import sys

url = None
if len(sys.argv) > 1:
    url = sys.argv[1]

if __name__ == "__main__":
    app.run(url)
