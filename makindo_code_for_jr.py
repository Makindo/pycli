import requests
import json
import pyodbc
import sys
import getopt
import time


# -b start at the beginning (grab the very first record), 
# -n next, pick up where you left off, 
# -o offset start at a supplied number
# defaults to mode = next
#example ways to strt program
#python makindo_api.py 
#python makindo_api.py -b
#python makindo_api.py -n
#python makindo_api.py -o --offset 20

#hacky pseudo global constants
GLOB_CONST_maxsqlerrors = 10
GLOB_CONST_maxmakindoerrors = 10
GLOB_CONST_h = {"Authorization":"Token token=\"a2eb02a8-8766-4e17-adf6-766c671050c3\"","Accept":"application/json","Content-Type":"application/json"}
GLOB_CONST_url = "https://api.makindo.io/matches"
GLOB_CONST_question_names = ['Q1','Q2','Q3','Q4'] #this should change with every survey. Questions gotten from Makindo that don't match this list of names are inaccurate!
GLOB_CONST_survey_id = 3 #this should change with every survey. We only want to slurp results for the given survey
GLOB_CONST_db_name = "makindo.makindo_match_rs"

#functions
def post_to_makindo(p_makindoid, p_match_type, p_makindo_errors):
  #routine that calls put to tell makindo if we were able to match the person or not
  l_makindo_put = '' #return value to indicate success or failur of communication with makindo
  l_puturl = GLOB_CONST_url+"/"+str(p_makindoid) #augmenting the global ural to look like https://api.makindo.io/matches/112
  l_putbody = "{\"status\":\""+p_match_type+"\"}"
  print 'puturl = ', l_puturl, ' putbody = ', l_putbody
  #Todo Rachel -- Do we need a try/catchand & timeout around this call
  l_r2 = requests.put(l_puturl, headers = GLOB_CONST_h, data = l_putbody, verify=False)
  print "l_r2.status = ", l_r2.status_code
  if 200 == l_r2.status_code:
    print "200 -- sucessfully reported to makindo"
    l_makindo_put = 'success'
  elif 404 == l_r2.status_code:
    print "404 Resource not found"
    l_makindo_put = 'failure'
    p_makindo_errors += 1
  elif 401 == l_r2.status_code:
    print "401 API key missing or not recognized "
    l_makindo_put = 'failure'
    p_makindo_errors += 1
  elif 409 == l_r2.status_code:
    print "409 The body of the request was malformed "
    print "Message from the server ", l_r2.text
    l_makindo_put = 'failure'
    p_makindo_errors += 1
  elif 422 == l_r2.status_code:
    print "422 There was something wring with the record. Reponse body contains errors "
    print "Message from the server ", l_r2.text
    l_makindo_put = 'failure'
    p_makindo_errors += 1
  elif 500 == l_r2.status_code: 
    print "500 A server problem prevented the request from succeeding"
    l_makindo_put = 'failure'
    p_makindo_errors += 1
  else:
    print "unknown makindo request code"
    l_makindo_put = 'failure'
    p_makindo_errors += 1   
  if GLOB_CONST_maxmakindoerrors <= p_makindo_errors:
     print "Too many makindo requests errors -- exiting "
     sys.exit(1)
  return (l_makindo_put, p_makindo_errors)

def blank_ques(p_input):
  #some of the variables like max_age come back as ? which messes with the mysql
  if '?' == p_input:
    p_input = ''
  return p_input

def is_number(p_input):
  #self evident
  try:
    float(p_input)
    return True
  except ValueError:
    return False

def get_survey_info(p_survey):
  questions = []
  answers = []
  if p_survey['id'] != GLOB_CONST_survey_id:
    print "wrong survey"
    return [[],[]]
  for q in p_survey["questions"]:
    if q["name"] in GLOB_CONST_question_names:
      questions.append(q["name"])
      answers.append(blank_ques(clean_string(q["answer"])))
  return [questions, answers]
  
  
def clean_string(p_input):

  if None == p_input:
    l_return_val = ''
  elif isinstance(p_input, str): 
    p_input = p_input.encode('latin-1', 'ignore')
    l_return_val = p_input.strip()
    p_input = blank_ques(p_input)
  elif isinstance(p_input, unicode):
    p_input = p_input.encode('latin-1', 'ignore')
    l_return_val = p_input.strip()
    p_input = blank_ques(p_input)
  elif is_number(p_input):
    l_return_val = str(p_input)
  else:
    l_return_val = ''
  #print "l_return_val = ", l_return_val
  return l_return_val

def get_mode():
  # Collects the arguments passed in
  # will set to beginning, next, or offset and will find offset if passed
  l_offset = 0
  try:
    l_opts, l_args = getopt.getopt(sys.argv[1:],"bno",["offset="])
  except getopt.GetoptError as err:
    print str(err)
    sys.exit(2)
  for l_opt, l_arg in l_opts:
    if '-b' == l_opt:
      print 'starting script at beginning'
      l_mode = 'beginning'
    elif '-n' == l_opt:
      l_mode = 'next'
      #offset will be set below
    elif l_opt in ("-o", "--offset"):
      l_mode = 'offset'
      l_offset3 = l_arg
      if 0 == len(l_offset3.strip()) or not(l_offset3.isdigit()):
        l_offset = 0
      else:
        l_offset = int(l_offset3)
    else:
      print 'mode unknown or unsupplied -- running in beginning mode'
  print 'mode = ', l_mode, ' offset = ', l_offset
  return (l_mode, l_offset)
  
def lookup_offset(p_cur):
  # selects the maximum makindoid from the database, then adds 1
  try:
    p_cur.execute("select max(makindoid) as makmax from makindo.makindo_match;")
    l_f3 = p_cur.fetchall()
  except pyodbc.Error:
    print "Error finding the old offset from mysql -- exiting"
    sys.exit(1) 
  if 1 == len(l_f3): 
    #print f3
    l_offset2 = '0'
    l_offset2 = l_f3[0].makmax
    print "offset = ", l_offset2
    if None == l_offset2 or not(l_offset2.isdigit()) or 0 == len(l_offset2.strip()):
      print "Invalid Offset returned from mysql -- exiting"
      sys.exit(1)
    else:
      l_offset = int(l_offset2)
      l_offset += 1
  else: #error
    #deal with error
    print "error looking up the max makindoid"
    sys.exit(1)
  return l_offset
  
def match_to_db(p_cur, p_state, p_city, p_firstname, p_lastname, p_other_locations, p_mysql_errors):
  #try to makindo person to db
  
  l_match_type = "ambigous"  # ToDo Rachel -- we really need a "failure" type
  l_person_id = 0 
  try:
    if p_city == 'United States' or ''  == p_city or p_city is None:
      p_cur.execute("select individualid from iusa_2013."+p_state+"_indiv_raw where firstname = ? and lastname = ?;",[p_firstname,p_lastname])
    else:
      p_cur.execute("select individualid from iusa_2013."+p_state+"_indiv_raw where firstname = ? and lastname = ? and city = ?;",[p_firstname,p_lastname,p_city])
    l_f = p_cur.fetchall()
    if len(l_f) == 1:
      #put the id and question results into a table
      print l_f
      l_person_id = l_f[0].individualid
      l_match_type = 'found'
    else:
      if len(p_other_locations) > 1:
        i = 0
        while i < len(p_other_locations):
          other_city = p_other_locations[i]['city']
          other_state = p_other_locations[i]['state']
          if other_city == 'United States' or ''  == other_city or other_city is None:
            p_cur.execute("select individualid from iusa_2013."+other_state+"_indiv_raw where firstname = ? and lastname = ?;",[p_firstname,p_lastname])
          else:
            p_cur.execute("select individualid from iusa_2013."+other_state+"_indiv_raw where firstname = ? and lastname = ? and city = ?;",[p_firstname,p_lastname,other_city])
          other_f = cur.fetchall()
          if len(other_f) == 1:
            print l_f
            l_person_id = l_f[0].individualid
            l_match_type = 'found'
            i = len(p_other_locations)
          i += 1
    if l_match_type != 'found':
      if len(l_f) > 1: #ambigous
        l_match_type = "ambiguous"     
      elif len(l_f) < 1: #no matches
        l_match_type = "missing"

  except pyodbc.Error:
    print 'PYODBC Error ', sys.exc_info()
    p_mysql_errors += 1
    if GLOB_CONST_maxsqlerrors <= p_mysql_errors:
      print 'allowable number of mysql errors exceeded'
      sys.exit(1)
      
  print " ".join([p_firstname,p_lastname,p_city,p_state,l_match_type])
  return(l_match_type, l_person_id, p_mysql_errors)

def write_res_to_db(p_cur, p_firstname, p_middlename, p_lastname, p_country, p_state, p_city, p_person_id, p_makindoid, p_makindoid2, p_age_min, p_age_max, p_match_type, p_makindo_put, p_questions, p_mysql_errors):
  # writes the person and matching iusa id to the makindo.makindo_match database
  if p_questions is not None and len(p_questions[0]) > 0:
    question_string = ","+",".join(p_questions[0])
    qmarks = ","+",".join(["?"]*len(p_questions[0]))
  try:
    p_cur.execute("select * from "+GLOB_CONST_db_name+" where firstname = ? and lastname = ? and state = ? and city = ?",[p_firstname, p_lastname, p_state, p_city])
    if len(cur.fetchall()) < 1:
      p_cur.execute("insert into "+GLOB_CONST_db_name+" (firstname, middlename, lastname, country, state, city, person_id, process_time, makindoid, makindoid2, age_min, age_max, match_type, makindo_put"+question_string+") values (?, ?, ?, ?, ?, ?, if(?, ?, null), sysdate(), ?, ?, ?, ?, ?, ?"+qmarks+");", [p_firstname, p_middlename, p_lastname, p_country, p_state, p_city, p_person_id, p_person_id, p_makindoid, p_makindoid2, p_age_min, p_age_max, p_match_type, p_makindo_put]+p_questions[1])
    else:
      print "record already seen, not inserted"
  except pyodbc.Error:
    print 'PYODBC Error ', sys.exc_info()
    p_mysql_errors += 1
    if GLOB_CONST_maxsqlerrors <= p_mysql_errors:
      print 'allowable number of mysql errors exceeded'
      sys.exit(1)
  return p_mysql_errors
  



#declare variables
mysql_errors = 0
makindo_errors = 0
mode = 'next' #the default mode
offset = 0

starttime = time.time()

#get the mode and offset  
mode, offset = get_mode()

#connect to the database
con = pyodbc.connect('DSN=xxx;UID=xxx;PWD=xxx') #server 7
cur = con.cursor()

#look up the newest record if mode = next
if 'next' == mode:
  offset = lookup_offset(cur)


matches = [0]
match_type = ''
person_id = ''
makindo_put = ''
while len(matches) > 0:
  # Todo Rachel -- if mode = next or offset -- we want to use the offset API instead
  # according to their old API the code snippet below should work, but doesn't
  # I guess they changed their API again
  #"https://api.makindo.io/matches?offset=215"
  #start offset code snippet
  #url = GLOB_CONST_url;
  #if 'beginning' == mode or 'offset' == mode:
  #  url = GLOB_CONST_url+"?offset="+str(offset)
  #  print " url = ", url
  #  
  #end offset code snippet
  
  #Todo Rachel -- Do we need a try/catch and & timeout around this call?
  print "start makindo records request -- time = ", time.time() - starttime
  r = requests.get(GLOB_CONST_url, headers = GLOB_CONST_h, verify=False)
  print "end makindo records request -- time = ", time.time() - starttime
  print r
  d = json.loads(r.text)
  matches = d['matches']
  for record in matches:
    print
    print record
    print
    #print "start request and return strings -- time = ", time.time() - starttime
    makindoid = clean_string(record['match']['id'])
    makindoid2 = clean_string(record['match']['person']['id'])
    firstname = clean_string(record['match']['person']['name'].split()[0])
    middlename = clean_string(" ".join(record['match']['person']['name'].split()[1:-1]))
    lastname = clean_string(record['match']['person']['name'].split()[-1])
    age_min = clean_string(record['match']['person']['age']['minimum'])
    age_max = clean_string(record['match']['person']['age']['maximum'])
    country = clean_string(record['match']['person']['location']['country'])
    state = clean_string(record['match']['person']['location']['state'])
    city = clean_string(record['match']['person']['location']['city'])
    other_locations = record['match']['person']['locations']
    questions = get_survey_info(record['match']['survey'])
    #print "end request and return strings -- time = ", time.time() - starttime
    
    #print "makindoid2 = ", makindoid2, " makindoid = ", makindoid, "firstname = ", firstname, " middlename =", middlename, " lastname = ", lastname, " age_min = ", age_min, " age_max = ", age_max, " country = ", country, " state = ", state, " city = ", city
    
    if len(state) != 2 :
      #we don't know what state to check against
      print state
      print record['match']['person']['id']
      match_type = 'ambigous' #this isn't really right...
    
    else:
      #match to the DB
      print "start match to db -- time = ", time.time() - starttime
      match_type, person_id, mysql_errors = match_to_db(cur, state, city, firstname, lastname, other_locations, mysql_errors)
      print "end match to db -- time = ", time.time() - starttime
      
    # push result back to makindo
    print "start report to makindo -- time = ", time.time() - starttime
    makindo_put, makindo_errors = post_to_makindo(makindoid, match_type, makindo_errors)
    print "end report to makindo -- time = ", time.time() - starttime
  
    #write the results to the database
    print "start write res to db -- time = ", time.time() - starttime
    mysql_errors = write_res_to_db(cur, firstname, middlename, lastname, country, state, city, person_id, makindoid, makindoid2, age_min, age_max, match_type, makindo_put, questions, mysql_errors)
    print "end write res to db -- time = ", time.time() - starttime

      
  url = d['meta']['next']
  print url
  
  

