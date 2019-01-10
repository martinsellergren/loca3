import xml.etree.cElementTree as ET
import urllib2
import sys
reload(sys)

if sys.version_info.major < 3:
    sys.setdefaultencoding('utf8')

languages = ['EN', 'SV']
lan = languages[0]
url = 'https://wiki.openstreetmap.org/wiki/Special:Export/Nominatim/Special_Phrases/{}'.format(lan)
root = ET.ElementTree(file=urllib2.urlopen(url)).getroot()
raw = root[1][3][7].text
raw = raw.replace('\n', '')
elems = raw.split('|-')

elems = elems[2:]
elems[-1] = elems[-1].split('|}')[0]
if (len(elems[-1].strip()) == 0):
    del elems[-1]

for elem in elems:
    parts = elem.split('||')
    word = parts[0][1:].strip()
    key = parts[1].strip()
    value = parts[2].strip()
    operator = parts[3].strip()
    plural = parts[4].strip()

    if operator == '-' and plural == 'N':
        print '{} | {} | {}'.format(word, key, value)
