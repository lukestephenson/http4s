package org.http4s
package server
package middleware

import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.{CRC32, Deflater}

import cats._
import cats.data.Kleisli
import fs2.Stream._
import fs2._
import fs2.compress._
import org.http4s.headers._
import org.log4s.getLogger

final class GZip[F[_] : Functor] private[middleware](
                                                      bufferSize: Int,
                                                      level: Int,
                                                      isZippable: Response[F] => Boolean
                                                    ) {
  private[this] val logger = getLogger

  def wrap(service: HttpService[F]): HttpService[F] =
    Kleisli { req =>
      req.headers.get(`Accept-Encoding`) match {
        case Some(acceptEncoding) if satisfiedByGzip(acceptEncoding) =>
          service.map(zipOrPass).apply(req)
        case _ => service(req)
      }
    }

  private def satisfiedByGzip(acceptEncoding: `Accept-Encoding`) =
    acceptEncoding.satisfiedBy(ContentCoding.gzip) || acceptEncoding.satisfiedBy(
      ContentCoding.`x-gzip`)

  private def zipOrPass(response: Response[F]): Response[F] =
    if (isZippable(response)) zipResponse(bufferSize, level, response)
    else response // Don't touch it, Content-Encoding already set

  private def zipResponse(
                           bufferSize: Int,
                           level: Int,
                           resp: Response[F]): Response[F] = {
    logger.trace("GZip middleware encoding content")
    // Need to add the Gzip header and trailer
    val trailerGen = new TrailerGen()
    val b = chunk(header) ++
      resp.body
        .through(trailer(trailerGen, bufferSize.toLong))
        .through(
          deflate(
            level = level,
            nowrap = true,
            bufferSize = bufferSize
          )) ++
      chunk(trailerFinish(trailerGen))
    resp
      .removeHeader(`Content-Length`)
      .putHeaders(`Content-Encoding`(ContentCoding.gzip))
      .copy(body = b)
  }

  private val GZIP_MAGIC_NUMBER = 0x8b1f
  private val GZIP_LENGTH_MOD = Math.pow(2, 32).toLong

  private val header: Chunk[Byte] = Chunk.bytes(
    Array(
      GZIP_MAGIC_NUMBER.toByte, // Magic number (int16)
      (GZIP_MAGIC_NUMBER >> 8).toByte, // Magic number  c
      Deflater.DEFLATED.toByte, // Compression method
      0.toByte, // Flags
      0.toByte, // Modification time (int32)
      0.toByte, // Modification time  c
      0.toByte, // Modification time  c
      0.toByte, // Modification time  c
      0.toByte, // Extra flags
      0.toByte
    ) // Operating system
  )

  private final class TrailerGen(val crc: CRC32 = new CRC32(), var inputLength: Int = 0)

  private def trailer(gen: TrailerGen, maxReadLimit: Long): Pipe[Pure, Byte, Byte] =
    _.pull.unconsLimit(maxReadLimit).flatMap(trailerStep(gen, maxReadLimit)).stream

  private def trailerStep(gen: TrailerGen, maxReadLimit: Long): (Option[
    (Segment[Byte, Unit], Stream[Pure, Byte])]) => Pull[Pure, Byte, Option[Stream[Pure, Byte]]] = {
    case None => Pull.pure(None)
    case Some((segment, stream)) =>
      val chunkArray = segment.toChunk.toArray
      gen.crc.update(chunkArray)
      gen.inputLength = gen.inputLength + chunkArray.length
      Pull.output(segment) *> stream.pull
        .unconsLimit(maxReadLimit)
        .flatMap(trailerStep(gen, maxReadLimit))
  }

  private def trailerFinish(gen: TrailerGen): Chunk[Byte] =
    Chunk.bytes(
      ByteBuffer
        .allocate(Integer.BYTES * 2)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(gen.crc.getValue.toInt)
        .putInt((gen.inputLength % GZIP_LENGTH_MOD).toInt)
        .array())
}

object GZip {

  // TODO: It could be possible to look for F.pure type bodies, and change the Content-Length header after
  // TODO      zipping and buffering all the input. Just a thought.
  def apply[F[_] : Functor](
                             service: HttpService[F],
                             bufferSize: Int = 32 * 1024,
                             level: Int = Deflater.DEFAULT_COMPRESSION,
                             isZippable: Response[F] => Boolean = defaultIsZippable(_ : Response[F])): HttpService[F] =
    new GZip[F](bufferSize, level, isZippable).wrap(service)

  def defaultIsZippable[F[_]](resp: Response[F]): Boolean = {
    val contentType = resp.headers.get(`Content-Type`)
    resp.headers.get(`Content-Encoding`).isEmpty &&
      (contentType.isEmpty || contentType.get.mediaType.compressible ||
        (contentType.get.mediaType eq MediaType.`application/octet-stream`))
  }

}
