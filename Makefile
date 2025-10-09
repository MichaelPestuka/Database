
all:
	g++ -g *.cpp -o database

run: all
	./database