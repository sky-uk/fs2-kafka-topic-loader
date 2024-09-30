package base

import utils.{EmbeddedKafka, KafkaHelpers}

abstract class KafkaSpecBase[F[_]] extends AsyncIntSpec[F], KafkaHelpers[F], EmbeddedKafka[F]
