package io.arnab

import java.util.*

object UUIDUtils {
    private val ID_CHARSET = "012KN7B6FGZ4XLMECDQ8PHJ9TRV35WSY".toCharArray()
    private const val BASE_NUM_BITS = 5
    private const val NUM_CHARS_LONG = (java.lang.Long.SIZE + BASE_NUM_BITS - 1) / BASE_NUM_BITS
    private const val MASK: Long = 0x0000001F

    private fun uuidBase32(uuid: UUID, builder: StringBuilder): StringBuilder {
        encode(uuid.mostSignificantBits, builder)
        encode(uuid.leastSignificantBits, builder)
        return builder
    }

    private fun encode(bits: Long, builder: StringBuilder): StringBuilder {
        var orig = bits
        val chars = CharArray(NUM_CHARS_LONG)
        for (i in NUM_CHARS_LONG - 1 downTo 0) {
            val lsb = (orig and MASK).toInt()
            chars[i] = ID_CHARSET.get(lsb)
            orig = orig ushr BASE_NUM_BITS
        }
        builder.append(chars)
        return builder
    }

    fun uuidBase32(prefix: String?): String {
        return uuidBase32(UUID.randomUUID(), StringBuilder(prefix)).toString()
    }
}