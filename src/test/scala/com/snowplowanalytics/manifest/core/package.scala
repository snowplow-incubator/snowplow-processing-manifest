package com.snowplowanalytics.manifest

import cats.data.EitherT
import cats.effect.IO

package object core {
  // IO is unexceptional here
  type ManifestIO[A] = EitherT[IO, ManifestError, A]

  object ManifestIO {
    def apply[A](a: A): ManifestIO[A] =
      EitherT.pure[IO, ManifestError](a)
  }

  implicit class IOGet[A](fa: ManifestIO[A]) {
    def getValue: Either[ManifestError, A] =
      fa.value.unsafeRunSync()
  }
}
