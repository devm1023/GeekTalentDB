from urllib.parse import quote

def urlify(comp):
    return quote(comp.lower())

def contains(list, filter):
    for x in list:
        if filter(x):
            return True
    return False

valid_alevels = ["Afrikaans", "Anthropology", "Arabic",
    "Archaeology", "Art and Design", "Bahasa", "Basque", 
    "Bengali", "Biology", "Business Studies", "Chemistry", 
    "Chinese", "Classical Civilisation", "Communication Studies", 
    "Computer Science", "Craft and Design", "Critical Thinking",
    "Czech", "Dance", "Danish", "Design", "Design and Technology",
    "Drama and Theatre Studies", "Dutch", "Economics", "Electronics",
    "Engineering", "English Language", "English Language and Literature",
    "English Literature", "Environmental Studies", "Fijian",
    "Film Studies", "Fine Art", "Finnish", "Food Technology", "French",
    "Further Mathematics", "Gaelic", "General Studies", "Geography",
    "Geology", "German", "Government and Politics", "Graphics", "Greek",
    "Gujurati", "Health and Social Care", "Hebrew", "Hindi", "History",
    "History of Art", "Hungarian", "ICT", "Irish", "Italian", "Japanese",
    "Latin", "Latvian", "Law", "Leisure and Recreation", "Malay",
    "Mathematics", "Media Studies", "Mongolian", "Music", "Nepali",
    "Norwegian", "Panjabi", "Performing Arts", "Persian", "Philosophy",
    "Photography", "Physical Education", "Physics", "Polish", "Portuguese",
    "Product Design", "Psychology", "Religious Studies", "Romanian", 
    "Russian", "Sanskrit", "Science", "Slovak", "Sociology", "Spanish", 
    "Statistics", "Syariah", "Tamil", "Textiles", "Travel and Tourism", 
    "Turkish", "Urdu", "Welsh", "World Development"]

if __name__ == "__main__":
    with open('whichuni_category_urls.txt', 'w') as cat:
        for alevel in valid_alevels:
            for alevel2 in valid_alevels:
                    if alevel != alevel2:
                        url = 'http://university.which.co.uk/a-level-explorer/' + urlify(alevel) + '/' + urlify(alevel2) + '\n'
                        cat.write(url)
    print('done')