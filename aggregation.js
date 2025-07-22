db.Carros.aggregate([

  // Junta a collection Montadoras com Carros em um array chamado PaisCarros
  {
    $lookup: {
      from: "Montadoras",
      localField: "Montadora",
      foreignField: "Montadora",
      as: "PaisCarros"
    }
  },

  // Desenrola o array PaisCarros
  { $unwind: "$PaisCarros" },

  // Seleciona os campos desejados
  {
    $project: {
      Carro:      1,
      Cor:        1,
      Montadora:  1,
      País:       "$PaisCarros.País"
    }
  },

  // Agrupa por Pais e cria o array de carros
  {
    $group: {
      _id:   "$País",
      Carros: {
        $addToSet: {
          Carro:     "$Carro",
          Cor:       "$Cor",
          Montadora: "$Montadora"
        }
      }
    }
  },

  // Ordena o resultado por País Alfabeticamente
  {
    $sort: { _id: 1 }
  }

]);
