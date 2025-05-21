In v0.100.3, we quietly rolled out support for GIL-free multi-threading for spaCy’s syntactic dependency parsing and named entity recognition models. Because these models take up a lot of memory, we’ve wanted to release the global interpretter lock (GIL) around them for a long time. When we finally did, it seemed a little too good to be true, so we delayed celebration — and then quickly moved on to other things. It’s now past time for a write-up.

This is mostly an implementation post, but to me, implementation is the pain and the product is the pleasure. So, let’s start with the pay-off. The pay-off is the `.pipe()` method, which adds _data-streaming_ capabilities to spaCy:

```python
Stream Parsing

import spacy

nlp = spacy.load('de')

for doc in nlp.pipe(texts, n_threads=16, batch_size=10000):

    analyse_text(doc)
```

Each document is processed independently, so if your batch size is large enough, and OpenMP is enabled, you should be able to work all your cores with only one copy of the spaCy models in memory. spaCy is designed for web-scale data processing — we want you to be able to perform sophisticated linguistic analysis on whole dumps of the Common Crawl. With effective shared memory parallelism, those jobs are many times cheaper.

| Method | Number threads | Seconds <sup>1</sup> |
| --- | --- | --- |
| Loop | 1 | 691s |
| Pipe | 1 | 678s |
| Pipe | 2 | 432s |
| Pipe | 4 | 312s |

## [Python, Cython and the Global Interpretter Lock](https://explosion.ai/blog/multithreading-with-cython#implementation)

Endless ink has been spilled about the CPython [Global Interpretter Lock (GIL)](https://wiki.python.org/moin/GlobalInterpreterLock). It isn’t a problem for most code, but for spaCy, it really is. Computers may be fast and getting faster, but the internet is big and getting bigger. We have a lot fo text to process, and we’d like to use our machines efficiently.

Python maintains reference counts in a global data structure. When you create or delete a Python object, its reference count has to change. However, the data structure holding the reference counts is not thread-safe. To change the reference counts, you therefore need to acquire the global interpretter lock.

One way around the GIL is therefore to avoid the need for Python variables. This is what I’ve done with spaCy. More specifically, spaCy is a Python library, but it’s not actually written in Python. It’s implemented in Cython, and transpiled into a C++ extension module.

In ordinary Python code, you can have a list of numbers like this:

```python
Python list

my_list = [0, 1, 2]
```

```c
Transpiled C

__pyx_t_1 = PyList_New(3); if (unlikely(!__pyx_t_1)) {__pyx_filename = __pyx_f[0]; __pyx_lineno = 1; __pyx_clineno = __LINE__; goto __pyx_L1_error;}

__Pyx_GOTREF(__pyx_t_1);

__Pyx_INCREF(__pyx_int_0);

__Pyx_GIVEREF(__pyx_int_0);

PyList_SET_ITEM(__pyx_t_1, 0, __pyx_int_0);

__Pyx_INCREF(__pyx_int_1);

__Pyx_GIVEREF(__pyx_int_1);

PyList_SET_ITEM(__pyx_t_1, 1, __pyx_int_1);

__Pyx_INCREF(__pyx_int_2);

__Pyx_GIVEREF(__pyx_int_2);

PyList_SET_ITEM(__pyx_t_1, 2, __pyx_int_2)
```

```python
C in Cython

from libc.stlib cimport malloc, free

my_arr = <int*>malloc(sizeof(int) * 3)

my_arr[0] = 1

my_arr[1] = 2

my_arr[2] = 3

do_stuff(my_arr)

free(my_arr)
```

The disadvantages of writing with `nogil` semantics are obvious — you’re limited to writing [C with (arguably) nicer syntax](https://explosion.ai/blog/writing-c-in-cython). If you’ve never tried it, I think it’s an interesting exercise to do without the Python semantics. It does make you appreciate what the language is providing. Probably the thing I miss most are the exceptions and the lists. The Python unicode object is also very useful.

## [Implementation of the Parser.pipe method](https://explosion.ai/blog/multithreading-with-cython#parser-pipe)

Here’s the implementation of the Parser.pipe method in spaCy. This method does the following:

-   Buffers the texts into temporary work arrays
-   Releases the GIL
-   Iterates over the work arrays in an OpenMP `prange` loop
-   Calls the `Parser.parseC()` method for each unit of work (each document)

```python
Parser.pipe

def pipe(self, stream, int batch_size=1000, int n_threads=2):

    cdef Pool mem = Pool()

    cdef TokenC** doc_ptr = <TokenC**>mem.alloc(batch_size, sizeof(TokenC*))

    cdef int* lengths = <int*>mem.alloc(batch_size, sizeof(int))

    cdef Doc doc

    cdef int i

    cdef int nr_class = self.moves.n_moves

    cdef int nr_feat = self.model.nr_feat

    cdef int status

    queue = []

    for doc in stream:

        doc_ptr[len(queue)] = doc.c

        lengths[len(queue)] = doc.length

        queue.append(doc)

        if len(queue) == batch_size:

            with nogil:

                for i in cython.parallel.prange(batch_size, num_threads=n_threads):

                    status = self.parseC(doc_ptr[i], lengths[i], nr_feat, nr_class)

                    if status != 0:

                        with gil:

                            sent_str = queue[i].text

                            raise ValueError("Error parsing doc: %s" % sent_str)

            PyErr_CheckSignals()

            for doc in queue:

                self.moves.finalize_doc(doc)

                yield doc

            queue = []

    batch_size = len(queue)

    with nogil:

        for i in cython.parallel.prange(batch_size, num_threads=n_threads):

            status = self.parseC(doc_ptr[i], lengths[i], nr_feat, nr_class)

            if status != 0:

                with gil:

                    sent_str = queue[i].text

                    raise ValueError("Error parsing doc: %s" % sent_str)

    PyErr_CheckSignals()

    for doc in queue:

        self.moves.finalize_doc(doc)

        yield doc
```

## [The hard part](https://explosion.ai/blog/multithreading-with-cython#the-hard-part)

I couldn’t tell you that multi-threading the parser was easy. At least, not with a straight face. I’ve never written a significant Java program, but I imagine writing multi-threaded Java is significantly easier. Using Cython, the task was at least possible. But it definitely wasn’t easy.

If you count my time in academia, I’ve been writing statistical parsers in Cython for a five or six years now, and I’ve always wanted to release the GIL around the parsing loop. By late 2015 I had the machine learning, hash table, outer parsing loop, and most of the feature extraction as `nogil` functions. But the [state object](https://github.com/explosion/spaCy/blob/0.100.2/spacy/syntax/stateclass.pxd#L10) had a complicated interface, and was implemented as a `cdef class`. I couldn’t create this object or store it in a container without acquiring the GIL.

The break-through came when I figured out an undocumented way to write a C++ class in Cython. This allowed me to hollow out the existing `cdef class` that controlled the parser state. I proxied its interface to the inner C++ class, method by method. This way I could keep the code working, and make sure I didn’t introduce any subtle bugs into the feature calculation.

You can see the inner class [here](https://github.com/explosion/spaCy/blob/master/spacy/syntax/_state.pxd). If you navigate around the git history of this file, you can see the patches where I implemented the `.pipe` method.

## [Conclusion](https://explosion.ai/blog/multithreading-with-cython#conclusion)

Natural language processing (NLP) programs have some peculiar performance characterstics. The algorithms and data structures involved are often rather complicated, and the numeric calculations being performed are often quite trivial.

spaCy’s parser and named entity recognition system have to make two or three predictions on each word of the input document, using a linear model. All of the complexity is in the feature extraction and state management code, and the computational bottle-neck ends up being [retrieval of the weights data](https://github.com/explosion/thinc/blob/master/thinc/linear/avgtron.pyx#L128) from main memory.

When we finally switch over to a neural network model, the considerations will be a little bit different. It’s possible to implement the parser such that you’re stepping forward through multiple sentences at once. However, that has its own challenges. I think it’s both easier and more effective to parallelise the outer loop, so I expect the work put into the current implementation will serve us well.
