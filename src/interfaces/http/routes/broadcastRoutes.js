const express = require("express");
// const router = express.Router();

module.exports = function createBroadcastRoutes(createBroadcastUseCase,auth) {
  const router = express.Router();
 
  
  router.post("/", 
    auth.valideToken(),
    auth.requireRole('agent_public'), // Ajuste le rôle selon tes besoins
    async (req, res) => {
    try {
      const { broadcastId, name, adminIds, recipientIds } = req.body;
      const broadcast = await createBroadcastUseCase.execute({
        broadcastId,
        name,
        adminIds,
        recipientIds,
      });
      res.status(201).json({ success: true, data: broadcast });
    } catch (error) {
      res.status(400).json({ success: false, error: error.message });
    }
  });
  
  /**
   * @api {get} /broadcasts/search Recherche globale messages/fichiers/conversations/groups/broadcast
   * @apiName SearchOccurrences
   * @apiGroup Broadcasts
   */
  router.get("/search", async (req, res) => {
    try {
      // Ajoute la logique si BroadcastController possède searchOccurrences
      await broadcastController.searchOccurrences(req, res);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Erreur lors de la recherche globale",
        error: error.message,
      });
    }
  });

  return router;
};
