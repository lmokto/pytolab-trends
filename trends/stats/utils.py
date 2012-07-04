import constants

def get_person_name(person_id):
    for person in constants.persons:
        if person[0] == person_id:
            return '%s %s' % (person[1], person[2])
