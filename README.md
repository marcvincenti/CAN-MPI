# Bidimensional CAN

This project is a bidimensional [Content Addressable Network](https://en.wikipedia.org/wiki/Content_addressable_network) with MPI.

## Usage

With MPI installed on your machine :

+ Build : `make`
+ Run : `make run`

Here, the data is composed of random floating points.

```cpp
typedef struct struct_donnee{
	float value;
	coordonnees coord;
}donnee;
```

## Limits

You have to remove the process like a stack because we canno't handle non-rectangular zones.
