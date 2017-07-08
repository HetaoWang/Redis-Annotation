/* Redis Object implementation.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include <math.h>
#include <ctype.h>

#ifdef __CYGWIN__
#define strtold(a,b) ((long double)strtod((a),(b)))
#endif

/* 创建一个robj对象，并分配内存 */
robj *createObject(int type, void *ptr) {
	robj *o = zmalloc(sizeof(*o)); 
	o->type = type; 
	o->encoding = OBJECT_ENCODING_RAW; 
	o->ptr = ptr; 
	o->refcount = 1; 

	/* Set the LRU to the current lruclock (minutes resolution). */ 
	o->lru = LRU_CLOCK(); 
	return o; 
}

/* Create a string object with encoding OBJ_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string. */
 robj *createRawStringObject(const char *ptr, size_t len) {
 	// 创建一个以SDS编码的字符串对象
 	return createObject(OBJ_STRING, sdsnewlen(ptr, len)); 
 }

/* Create a string object with encoding OBJ_ENCODING_EMBSTR, that is
 * an object where the sds string is actually an unmodifiable string
 * allocated in the same chunk as the object itself. */ 
/* OBJ_ENCODING_EMBSTR编码的字符串的格式为 <robj><sdshdr8><buf><end>
 * 其中 <buf> 为 字符串实体，为<sdshdr8>中的buf字段
 * <end> 为'\0'，表示整个对象的结束。robj的ptr字段指向<buf>的起始位置 */
 robj *createEmbeddedStringObject(const char *ptr, size_t len) {
 	robj *o = zmalloc(sizeof(robj) + sizeof(sdshdr8) + len + 1); 
 	struct sdshdr8 *sh = (void *)(o + 1);     // 指针o的数据类型为robj，所以 o+1 会将指针前移sizeof(robj)字节，即指向<sdshdr8>的起始位置

 	o->type = OBJ_STRING; 
 	o->encoding = OBJ_ENCODING_EMBSTR; 
 	/* 指针sh的数据类型为sdshdr8，sh + 1 会将指针前移sizeof(sdshdr8)字节。sdshdr8结构的最后一个字段buf为字符数组，sizeof(sdshdr8)的时候不计算内存空间 
 	 * 所以 sh + 1 = sh->buf, o->ptr = sh->buf */
 	o->ptr = sh + 1;        
 	o->refcount = 1; 
 	o->lru = LRU_CLOCK(); 

  	sh->len = len; 
  	sh->alloc = len; 
  	sh -> flags = SDS_TYPE_8; 
  	if (ptr) {
  		memcpy(sh->buf, ptr, len); 
  		sh->buf[len] = '\0';         // 最后一字节置为'\0'
  	} else {
  		memset(sh->buf, 0, len + 1); 
  	}
 }

/* Create a string object with EMBSTR encoding if ti is smaller than 
 * REDIS_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is 
 * used. 
 * The current limit of 39 is chosen so that the biggest string object
 * we allocate as EMBSTR still fit into the 64 byte arena of jmalloc. */ 
// 这里注释写错了？明明是44个字节。。。
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44
 robj *createStringObject(const char *ptr, size_t len) {
 	if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT) {
 		return createEmbeddedStringObject(ptr, len); 
 	} else {
 		return createRawStringObject()
 	}
 }

robj *createStringObjectFromLongLong(long long value) {
	robj *o; 
	if (value >= 0 && value < OBJ_SHARED_INTEGERS) {
		incrRefCount(shared.integers[value]); 
	} else {
		if (value >= LONG_MIN && value <= LONG_MAX) {    
			// OBJ_ENCODING_INT 编码表示的值范围为 LONG_MIN ~ LONG_MAX
			o = createObject(OBJ_STRING, NULL); 
			o->encoding = OBJ_ENCODING_INT; 
			o->ptr = (void *)((long) value); 
		} else {
			// 超过long范围的值用 OBJ_ENCODING_RAW编码
			o = createObject(OBJ_STRING, sdsfromlonglong(value)); 
		}
	}
	return o; 
}

/* Create a string object from a long double. If humanfriendly is non-zero
 * it does not use exponential format and trims trailing zeroes at the end, 
 * however this results in loss of precision. Otherwise exp format is used
 * and the output of snprintf() is not modified. 
 *
 * The 'humanfriendly' option is used for INCRBYFLOAT and HINCRBYFLOAT. */
robj *createStringObjectFromLongDouble(long double value, int humanfriendly) {
	char buf[256]; 
	int len; 

	if (isinf(value)) {
		/* Libc in odd systems (Hi Solaris!) will format infinite in a
        * different way, so better to handle it in an explicit way. */
        if (value > 0) {
        	memcpy(buf, "inf", 3); 
        	len = 3; 
        } else {
        	memcpy(buf, "-inf", 4); 
        	len = 4; 
        }
	} else if (humanfriendly) {
		/* We use 17 digits precision since with 128 bit floats that precision
		 * after rounding is able to represent most small decimal numbers in a 
		 * way that is "non suprising" for the user (that is, most small 
		 * decimal numbers will be represented in a way that when converted 
		 * back into a string are exactly the same as what the user typed.) */
		len = snprintf(buf, sizeof(buf), "%.17Lf", value); 
		/* Now remove trailing zeros after the  */
		if (strchr(buf, '.') != NULL) {
			char *p = buf + len - 1; 
			while (*p == '0') {
				p--; 
				len--; 
			}
			if (*p == '.') len--; 
		}
	} else {
		len = snprintf(buf, sizeof(buf), "%.17Lg", value); 
	}
	return createStringObject(buf, len); 
}

/* Duplicate a string object, with the guarantee that the returned object
 * hash the same encoding as the original one. 
 *
 * This function also guarantees that duplicating a small integer object 
 * (or a string object contains a representation of a small integer)
 * will always result in a fresh object that is unshared (refcount == 1)
 * 对于 <OBJ_SHARED_INTEGERS 的整数，该函数也会创建一个新的robj，而不会复用 shared integer
 * 
 * The resulting object always has refcount set to 1. */
robj *dupStringObject(robj *o) {
	robj *d; 

	serverAssert(o->type == OBJ_STRING); 

	switch (o->encoding) {
		case OBJ_ENCODING_RAW: 
			return createRawStringObject(o->ptr, sdslen(o->ptr)); 
		case OBJ_ENCODING_EMBSTR: 
			return createEmbeddedStringObject(o->ptr, sdslen(o->ptr)); 
		case OBJ_ENCODING_INT: 
			d = createObject(OBJ_STRING, NULL); 
			d->encoding = OBJ_ENCODING_INT; 
			d->ptr = o->ptr; 
			return d; 
		default: 
			serverPanic("Wrong encoding."); 
			break; 
	}
}

// 创建一个list对象，底层使用quicklist结构存储
robj *createQuicklistObject(void) {
	quicklist *l = quicklistCreate(); 
	robj *o = createObject(OBJ_LIST, l); 
	o->encoding = OBJ_ENCODING_QUICKLIST; 
	return o; 
}

// 创建一个list对象，底层使用ziplist存储
robj *createZiplistObject(void) {
	unsigned char *zl = ziplistNew(); 
	robj *o = createObject(OBJ_LIST, zl); 
	o->encoding = OBJ_ENCODING_ZIPLIST; 
	return o; 
}

// 创建一个set对象，底层使用字典存储
robj *createSetObject(void) {
	dict *d = dictCreate(&setDictType, NULL); 
	robj *o = createObject(OBJ_SET, d); 
	o->encoding = OBJ_ENCODING_HT; 
	return o; 
}

// 创建一个set对象，底层使用intset存储
robj *createIntsetObject(void) {
	intset *is = intsetNew(); 
	robj *o = createObject(OBJ_SET, is); 
	o->encoding = OBJ_ENCODING_INTSET; 
	return o; 
}

// 创建一个hash对象，底层使用ziplist存储
robj *createHashObject(void) {
	unsigned char *zl = ziplistNew(); 
	robj *o = createObject(OBJ_HASH, zl); 
	o->encoding = OBJ_ENCODING_ZIPLIST; 
	return o; 
}

// 创建一个zset对象，底层使用skiplist + dict存储
robj *createZsetObject(void) {
	zset *zs = zmalloc(sizeof(*zs)); 
	robj *o; 

	zs->dict = dictCreate(&zsetDictType, NULL); 
	zs->zsl = zslCreate(); 
	o = createObject(OBJ_ZSET, zs); 
	o->encoding = OBJ_ENCODING_SKIPLIST; 
	return o; 
}

// 创建一个zset对象，底层使用ziplist存储
robj *createZsetZiplistObject(void) {
	unsigned char *zl = ziplistNew(); 
	robj *o = createObject(OBJ_ZSET, zl); 
	o->encoding = OBJ_ENCODING_ZIPLIST; 
	return o; 
}



/* 以free开头的函数用来释放obj对象所管理的数据结构，但不释放对象本身
 * Redis支持的5中数据类型，各自有自己的free方法： 
 * OBJ_STRING OBJ_LIST OBJ_HASH OBJ_SET OBJ_ZSET */

void freeStringObject(robj *o) {
	if (o->encoding == OBJ_ENCODING_RAW) {
		// 由于OBJ_ENCODING_EMBSTR和 OBJ_ENCODING_INT编码的str，str本身与obj存储在一起，
		// 所以直接释放obj即可，不需要单独处理
		sdsfree(o->ptr); 
	}
}

/* list支持的编码方式只有 quicklist ？？ */
void freeListObject(robj *o) {
	if (o->encoding == OBJ_ENCODING_QUICKLIST) {
		quicklistRelease(o->ptr); 
	} else {
		serverPanic("Unknown list encoding type"); 
	}
}

/* set支持的编码方式有 OBJ_ENCODING_HT 和 OBJ_ENCODING_INTSET */
void freeSetObject(robj *o) {
	switch (o->encoding) {
		case OBJ_ENCODING_HT: 
			dictRelease((dict *) o->ptr); 
			break; 
		case OBJ_ENCODING_INTSET: 
			zfree(o->ptr); 
			break; 
		default: 
			serverPanic("Unknown set encoding type"); 
	}
}

/* zset支持的编码方式有 skiplist+dict 和 ziplist */
void freeZsetObject(robj *o) {
	zset *zs; 
	switch (o->encoding) {
		case OBJ_ENCODING_SKIPLIST: 
			zs = o->ptr; 
			dictRelease(zs->dict); 
			zslFree(zs->zsl); 
			zfree(zs); 
			break; 
		case OBJ_ENCODING_ZIPLIST: 
			zfree(o->ptr); 
			break;
		default: 
			serverPanic("Unknown sorted set encoding"); 
	}
}

/* hash支持的编码方式有dict和ziplist */
void freeHashObject(robj *o) {
	switch (o->encoding) {
		case OBJ_ENCODING_HT: 
			dictRelease((dict*) o->ptr); 
			break; 
		case OBJ_ENCODING_ZIPLIST: 
			zfree(o->ptr); 
			break; 
		default: 
			serverPanic("Unknown hash encoding type"); 
	}
}

void incrRefCount(robj *o) {
	o->refcoung++; 
}

void decrRefCount(robj *o) {
	if (o->refcount <= 0) serverPanic("decrRefCount against refcount <= 0"); 
	if (o->refcount == 1) {
		switch (o->type) {
			case OBJ_STRING: freeStringObject(o); break; 
			case OBJ_LIST: freeListObject(o); break; 
			case OBJ_SET: freeSetObject(o); break; 
			case OBJ_ZSET: freeZsetObject(o); break; 
			case OBJ_HASH: freeHashObject(o); break; 
			default: serverPanic("Unknown object type"); break; 
		}
		zfree(o);        // 释放对象本身
	} else {
		o->refcount--;
	}
}

/* This variant of decrRefCount() gets its argument as void, and is useful 
 * as free method in data structs that expect a 'void free_object(void *)'
 * prototype for the free method. */ 
void decrRefCountVoid(void *o) {
	decrRefCount(o); 
}

/* This function set the ref count to zero without freeing the object. 
 * It is useful in order to pass a new object to functions incrementing 
 * the ref count of the received object. Example: 
 * 	  functionThatWillIncrementRefCount(resetRefCount(createObject(...)))
 * Otherwise you need to resort to the less elegant pattern: 
 *    
 *    *obj = createObject(); */
robj *resetRefCount(robj *o) {
	o->refcount = 0; 
	return o; 
}

/* 
 * 检查对象o的类型是否为指定类型。如果类型不匹配就会给客户端发出一条警告并返回1.
 * shared.wrongtypeerr的内容是“-WRONGTYPE Operation against a key holding the wrong kind of value”
 */
int checkType(client *c, robj *o, int type) {
	if (o-> type != type) {
		addReply(c, shared.wrongtypeerr); 
		return 1; 
	}
	return 0; 
}



