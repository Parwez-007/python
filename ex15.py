from sys import argv
# Argument names - To be entered manually when running the program
script, filename = argv
# Setting the text file to the variable txt
txt = open(filename)
# Prints a message and then reading the text
print(f"Here's your file {filename}:")
print(txt.read())

txt.close()
