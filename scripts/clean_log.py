from imdb import IMDb

def read_log(fname):
    remove_dict = {}
    with open(fname) as f:
        for line in f.readlines():
            line_list = line.split(" ")
            key = line_list[1] + "-" + line_list[4]
            if key in remove_dict:
                remove_dict[key] += 1
            else:
                remove_dict[key] = 1
            print(key)
    return remove_dict
# create an instance of the IMDb class
ia = IMDb()


# remove = read_log("../data/log.txt")

# get a movie
movie = ia.get_movie('0133093')

# print the names of the directors of the movie
print('Directors:')
for director in movie['directors']:
    print(director['name'])

# print the genres of the movie
print('Genres:')
for genre in movie['genres']:
    print(genre)

# search for a person name
people = ia.search_person('Mel Gibson')
for person in people:
   print(person.personID, person['name'])