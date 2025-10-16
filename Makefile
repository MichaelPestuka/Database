
all:
	g++ -g src/*.cpp -o database

run: all
	./database