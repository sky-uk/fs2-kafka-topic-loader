package base

import utils.{EmbeddedKafka, KafkaHelpers}

abstract class KafkaSpecBase[F[_]] extends AsyncIntSpec with KafkaHelpers[F] with EmbeddedKafka[F]
