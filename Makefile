
all:
	g++ -g -std=c++20 *.cpp -o database

run: all
	./database