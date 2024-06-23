import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ps.core.aqp import AQP

def main():
    query = "SELECT id,price FROM users AS u JOIN companies ON u_id = c_id WHERE age > 21 AND (sex = 'M' OR age < 34) GROUP BY company"
    aqp = AQP(query)

if __name__ == "__main__":
    main()
