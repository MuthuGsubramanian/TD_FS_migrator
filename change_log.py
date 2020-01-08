import datetime


change_header = '''// CREATE/CHANGE LOG : '''
change_title = '''// DATE                  MOD BY                               GCC                           DESC'''
change_content = '''{date}              {id}                          {version}                      {desc}'''
change_tail = '''//     **********************************************************************************************'''

change_date = str(datetime.datetime.today().date())
change_id = 'TD_SF_Engine'
ver = 'initial'
des = 'macro'

op_log = change_header + '\n' + change_title + '\n' + change_content.format(date=change_date,
                                                                        id=change_id,
                                                                        version=ver,
                                                                        desc=des) + '\n' + change_tail + '\n\n'
# op = change_header + '\n' + change_title + '\n' + change_content + '\n' + change_title