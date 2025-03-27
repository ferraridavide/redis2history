import unittest
from plugins.drivers_downsampling import downsampling
import datetime
from plugins.trucklink_models import DriverValues, Position


class TestDriverDownsampling(unittest.TestCase):

    # Run with python -m unittest unittests.TestDriverDownsampling
    def setUp(self):
        self.outOfScopeCriteria = lambda sample: sample.out_of_scope == "true"
        self.currentStatus = lambda sample: sample.status
        self.prevStatus = lambda sample: sample.prevStatus

        self.otherArgs = {
            "position": Position(latitude="45.0", longitude="9.0", altitude="0.0", hdop="0.0", angle="0.0", speed="0.0"),
            "vin": "my_vin",
            "tacho_slot": "my_tacho_slot"
        }

    def test_out_of_scope_rispettato(self):
        """
        Caso di test per verificare il comportamento della funzione di downsampling quando il flag `out_of_scope` è impostato su `true`.

        Assert:
        - La lunghezza dei dati di output dovrebbe essere 1.
        - L'attributo `out_of_scope` del primo oggetto DriverValues nei dati di output dovrebbe essere `true`.
        - L'attributo `status` del primo oggetto DriverValues nei dati di output dovrebbe essere `⚠️`.
        """
        # --> Arrange
        input_data = {
            datetime.datetime(2024, 3, 14, 0, 0, 00): DriverValues(status="✅", prevStatus="A", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 10): DriverValues(status="✅", prevStatus="A", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 20): DriverValues(status="⚠️", prevStatus="A", out_of_scope="true", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 30): DriverValues(status="✅", prevStatus="A", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 40): DriverValues(status="✅", prevStatus="A", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 50): DriverValues(status="✅", prevStatus="A", out_of_scope="false", **self.otherArgs),
        }

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)

        # --> Assert
        self.assertEqual(len(output_data), 1)
        self.assertEqual(list(output_data.values())[0].out_of_scope, "true")
        self.assertEqual(list(output_data.values())[0].status, "⚠️")

    def test_minuto_precedente_rispettato(self):
        """
        Caso di test per verificare il comportamento della funzione di downsampling quando il 
        campione del minuto precedente è presente.
        """
        # --> Arrange
        input_data = {
            # --> Gruppo 1
            datetime.datetime(2024, 3, 14, 0, 0, 00): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 40): DriverValues(status="🟢", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 50): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            # --> Gruppo 2
            datetime.datetime(2024, 3, 14, 0, 1, 40): DriverValues(status="🔴", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 1, 45): DriverValues(status="🔵", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 1, 50): DriverValues(status="🟢", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
        }

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)
        
        # --> Assert
        self.assertEqual(len(output_data), 2)
        self.assertEqual(list(output_data.values())[0].status, "🔴")
        self.assertEqual(list(output_data.values())[1].status, "🔴")

    def test_minuto_precedente_sample_non_presente_prende_secondo_migliore(self):
        # --> Arrange
        input_data = {
            # --> Gruppo 1
            datetime.datetime(2024, 3, 14, 0, 0, 00): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 40): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 50): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            # --> Gruppo 2
            datetime.datetime(2024, 3, 14, 0, 1, 40): DriverValues(status="🔵", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 1, 50): DriverValues(status="🟢", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
        }

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)
        
        # --> Assert
        self.assertEqual(len(output_data), 2)
        self.assertEqual(list(output_data.values())[0].status, "🔴")
        self.assertEqual(list(output_data.values())[1].status, "🟢") # --> Questo è l'ultimo sample tra i due (pareggio) tra i secondi migliori

    def test_prende_ultimo_sample(self):
        """
        Caso di test della funzione di downsampling per verificare che venga estratto l'ultimo 
        sample dello status prevalente.
        """
        # --> Arrange
        input_data = {
            datetime.datetime(2024, 3, 14, 0, 0, 00): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 10): DriverValues(status="🔵", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 20): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 30): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 40): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 50): DriverValues(status="🟢", prevStatus="🔵", out_of_scope="false", **self.otherArgs)
        }

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)
        
        # --> Assert
        self.assertEqual(len(output_data), 1)
        self.assertEqual(list(output_data.keys())[0], datetime.datetime(2024, 3, 14, 0, 0, 40))

        pass

    def test_nessun_campione(self):
        # --> Arrange
        input_data = {}

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)

        # --> Assert
        self.assertEqual(len(output_data), 0)

    def test_pareggio(self):
        # --> Arrange
        input_data = {
            # --> Gruppo 1
            datetime.datetime(2024, 3, 14, 0, 0, 00): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 15): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 30): DriverValues(status="🟢", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
        }

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)
        
        # --> Assert
        self.assertEqual(len(output_data), 1)
        self.assertEqual(list(output_data.values())[0].status, "🟢")

    def test_maggioranza_prev_status(self):
        """
        Caso di test per esaminare il funzionamento della funzione di downsampling quando prevStatus
        ha la durata più lunga.
        """
        # --> Arrange
        input_data = {
            # --> Gruppo 1
            datetime.datetime(2024, 3, 14, 0, 0, 00): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 40): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 50): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            # --> Gruppo 2
            datetime.datetime(2024, 3, 14, 0, 1, 40): DriverValues(status="🔴", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 1, 45): DriverValues(status="🔵", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
        }

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)
        
        # --> Assert
        self.assertEqual(len(output_data), 2)
        self.assertEqual(list(output_data.values())[1].status, "🔴")

    def test_nessun_sample_come_prev_status(self):
        """
        Caso di test per esaminare il funzionamento della funzione di downsampling quando non esiste
        alcun campione nel gruppo con lo stesso status di prevStatus, nonostante prevStatus abbia la
        durata più lunga.
        """
        # --> Arrange
        input_data = {
            # --> Gruppo 1
            datetime.datetime(2024, 3, 14, 0, 0, 00): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 40): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 0, 50): DriverValues(status="🔴", prevStatus="🔵", out_of_scope="false", **self.otherArgs),
            # --> Gruppo 2
            datetime.datetime(2024, 3, 14, 0, 1, 40): DriverValues(status="🔵", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 1, 50): DriverValues(status="🔵", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
            datetime.datetime(2024, 3, 14, 0, 1, 55): DriverValues(status="🟢", prevStatus="🔴", out_of_scope="false", **self.otherArgs),
        }

        # --> Act
        output_data = downsampling(
            input_data, 60, self.outOfScopeCriteria, self.currentStatus, self.prevStatus)
        
        # --> Assert
        self.assertEqual(len(output_data), 2)
        self.assertEqual(list(output_data.values())[1].status, "🔵")

## Più out-of-scope nello stesso gruppo, viene preso l'ultimo
if __name__ == '__main__':
    unittest.main()
