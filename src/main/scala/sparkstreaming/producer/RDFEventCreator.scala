package sparkstreaming.producer

/**
  * Created by xiangnanren on 25/05/16.
  */

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, Locale}

import org.openrdf.model.ValueFactory
import org.openrdf.model.impl.{LinkedHashModel, SimpleValueFactory}

import scala.util.Random


/**
  * Created by a612504 on 14/03/2016.
  */
object RDFEventCreator {

  def apply(obsId: Long) = {
    val vf: ValueFactory = SimpleValueFactory.getInstance()
    val model = new LinkedHashModel()
    val event: RDFEvent = TripleEvent(model)

    model.add(vf.createIRI("http://ontology.waves.org/Observation_" + obsId),
      vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
      vf.createIRI("http://purl.oclc.org/NET/ssnx/ssn/ObservationValue"))

    model.add(vf.createIRI("http://ontology.waves.org/Observation_" + obsId),
      vf.createIRI("http://purl.oclc.org/NET/ssnx/ssn/startTime"),
      vf.createLiteral(date(): Date))

    model.add(vf.createIRI("http://ontology.waves.org/Observation_" + obsId),
      vf.createIRI("http://data.nasa.gov/qudt/owl/qudt/unit"),
      vf.createLiteral("http://www.units.org/2016/unit#lh"))

    model.add(vf.createIRI("http://ontology.waves.org/Observation_" + obsId),
      vf.createIRI("http://data.nasa.gov/qudt/owl/qudt/numericValue"),
      vf.createLiteral(double(): Double))

    model.add(vf.createIRI("http://localhost:80/waves/stream/1i/" + obsId),
      vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
      vf.createIRI("http://purl.oclc.org/NET/ssnx/ssn/SensorOutput"))

    model.add(vf.createIRI("http://localhost:80/waves/stream/1i/" + obsId),
      vf.createIRI("http://purl.oclc.org/NET/ssnx/ssn/isProducedBy"),
      vf.createIRI("http://www.zone-waves.fr/2016/sensor#CPT_RESEAU_AVE_BURAGO_DI_MALGORA"))

    model.add(vf.createIRI("http://localhost:80/waves/stream/1i/" + obsId),
      vf.createIRI("http://purl.oclc.org/NET/ssnx/ssn/hasValue"),
      vf.createIRI("http://ontology.waves.org/Observation_" + obsId))

    event
  }


  def double(): Double = {
    val min: Int = 10
    val max: Int = 60
    (Random.nextInt(max) % (max - min + 1) + min).toDouble / 100
  }

  def date(): Date = {
    val dstr: String = "2001-07-04T12:08:56.235-0700"
    val format: DateFormat =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH)
    val d = format.parse(dstr)
    d
  }
}




