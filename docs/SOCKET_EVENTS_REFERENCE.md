# Référence des Événements Socket.IO — ChatHandler

> Dernière mise à jour : 7 avril 2026

Ce document liste **tous les événements Socket.IO** gérés par le `chatHandler`, avec leurs **données d'entrée**, **ACK de succès**, **ACK d'erreur**, **broadcasts** associés, et les **émissions asynchrones du `MessageDeliveryService` (MDS)** via Redis Streams.

---

## Table des matières

- [Connexion & Authentification](#connexion--authentification)
- [Messages](#messages)
- [Conversations](#conversations)
- [Groupes & Diffusion](#groupes--diffusion)
- [Appels (CALL / VIDEO_CALL)](#appels-call--video_call)
- [Participants](#participants)
- [Présence & Surveillance](#présence--surveillance)
- [Rôles](#rôles)
- [Messages — Gestion (édition, suppression)](#messages--gestion-édition-suppression)
- [Fichiers](#fichiers)
- [Réactions](#réactions)
- [Réponses](#réponses)
- [Transfert](#transfert)
- [Typing](#typing)
- [Utilitaires (ping, heartbeat)](#utilitaires-ping-heartbeat)
- [Récapitulatif — Émissions MessageDeliveryService (MDS)](#récapitulatif--émissions-messagedeliveryservice-mds)
- [Architecture Multi-Device (senderSocketId)](#architecture-multi-device-sendersocketid)

---

## Légendes

| Symbole       | Signification                                                                            |
| ------------- | ---------------------------------------------------------------------------------------- |
| 🔄 **MDS →**  | Événement émis de façon **asynchrone** par le `MessageDeliveryService` via Redis Streams |
| ~~Broadcast~~ | Broadcast direct **supprimé** — remplacé par la distribution MDS                         |
| ⏭️            | Socket émetteur **exclu** de la livraison MDS (reçoit uniquement l'ACK)                  |

---

## Connexion & Authentification

### `authenticate`

|                  |                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**       | `{ token?, userId?, matricule?, nom?, prenom?, ministere?, departement? }`                                                       |
| **ACK succès**   | `authenticated` → `{ success, userId, matricule, nom, prenom, ministere, autoJoinedConversations, timestamp }`                   |
| **ACK erreur**   | `auth_error` → `{ message, code }`                                                                                               |
| **Side effects** | Rejoint les rooms `user_<id>`, `conversation_<id>`, `ministere_<nom>`. Émet `conversationsLoaded` avec toutes les conversations. |

### `disconnect`

|                         |                                                                                  |
| ----------------------- | -------------------------------------------------------------------------------- |
| **Entrée**              | _(automatique, raison fournie par Socket.IO)_                                    |
| **Broadcast**           | `user_disconnected` → `{ userId, matricule, timestamp, reason }`                 |
| **⏭️ Émetteur exclu ?** | ✅ Oui — `socket.broadcast.emit()` exclut nativement le socket qui se déconnecte |

---

## Messages

### `sendMessage`

|                      |                                                                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**           | `{ content, conversationId, type?, receiverId?, conversationName?, temporaryId?, fileId?, callMetadata? }`                    |
| **ACK succès**       | `message_sent` → `{ success, messageId, message, conversation, temporaryId, status, timestamp }`                              |
| **ACK erreur**       | `message_error` → `{ message, code, error? }`                                                                                 |
| 🔄 **MDS → privé**   | `newMessage` → destinataire + ⏭️ autres appareils émetteur                                                                    |
| 🔄 **MDS → groupe**  | `message:group` → participants (⏭️ socket émetteur exclu)                                                                     |
| 🔄 **MDS → canal**   | `message:channel` → participants (⏭️ socket émetteur exclu)                                                                   |
| 🔄 **MDS → système** | `newMessage` → tous les participants                                                                                          |
| **senderSocketId**   | ✅ Propagé : chatHandler → SendMessage → ResilientMessageService → MDS                                                        |
| **Side effects**     | Crée la conversation si inexistante. Publie dans Redis Streams → le MDS livre aux destinataires via les événements ci-dessus. |

### `getMessages`

|                |                                                    |
| -------------- | -------------------------------------------------- |
| **Entrée**     | `{ conversationId, page?, limit? }`                |
| **ACK succès** | `messagesLoaded` → `{ messages, pagination, ... }` |
| **ACK erreur** | `messages_error` → `{ message, code }`             |

### `messages:quickload`

|                |                                                                                  |
| -------------- | -------------------------------------------------------------------------------- |
| **Entrée**     | `{ conversationId, limit? }`                                                     |
| **ACK succès** | `messages:quick` → `{ conversationId, messages, hasMore, fromCache, timestamp }` |
| **ACK erreur** | `messages:error` → `{ error, code }`                                             |

### `messages:fullload`

|                |                                                                  |
| -------------- | ---------------------------------------------------------------- |
| **Entrée**     | `{ conversationId, cursor?, limit? }`                            |
| **ACK succès** | `messages:full` → `{ conversationId, messages, ..., timestamp }` |
| **ACK erreur** | `messages:error` → `{ error, code }`                             |

### `markMessageDelivered`

|                    |                                                                                                                                                                            |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ messageId, conversationId? }`                                                                                                                                           |
| **ACK**            | _(pas d'émission directe — le use case publie dans Redis Streams)_                                                                                                         |
| 🔄 **MDS →**       | `message:status` → `{ messageId, conversationId, userId, status: "DELIVERED", participants, timestamp }`                                                                   |
| **senderSocketId** | ⚠️ Non propagé — **OK par design** : le DELIVERED est envoyé à l'expéditeur original du message, pas au lecteur. L'expéditeur doit le recevoir sur **tous** ses appareils. |

### `markMessageRead`

Supporte **deux formats** : message unique ou batch (tableau d'IDs).

|                     |                                                                                                                       |
| ------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **Entrée (unique)** | `{ messageId, conversationId? }`                                                                                      |
| **Entrée (batch)**  | `{ conversationId, messageIds: ["id1", "id2", ...] }`                                                                 |
| **Validation**      | Requiert soit `messageId`, soit `conversationId` + `messageIds` (tableau non vide)                                    |
| **ACK**             | _(pas d'émission directe — le use case publie dans Redis Streams)_                                                    |
| 🔄 **MDS →**        | `message:status` → `{ messageId, conversationId, userId, status: "READ", participants, timestamp }`                   |
| 🔄 **MDS → bulk**   | `message:status` → `{ isBulk: true, conversationId, userId, status: "READ", messageCount, participants, timestamp }`  |
| **senderSocketId**  | ⚠️ Non propagé — **OK par design** : même raisonnement que DELIVERED.                                                 |
| **Side effects**    | Décrémente `userMetadata.unreadCount` du nombre exact de messages lus (`modifiedCount`), au lieu de réinitialiser à 0 |

---

## Conversations

### `getConversations`

|                |                                                                                                                         |
| -------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Entrée**     | `{ page?, limit? }`                                                                                                     |
| **ACK succès** | `conversationsLoaded` → `{ conversations, pagination, totalUnreadMessages, unreadConversations, fromCache, timestamp }` |
| **ACK erreur** | `conversations_error` → `{ message, code }`                                                                             |

### `getConversation`

|                |                                                                               |
| -------------- | ----------------------------------------------------------------------------- |
| **Entrée**     | `{ conversationId }`                                                          |
| **ACK succès** | `conversationLoaded` → `{ conversation, metadata: { fromCache, timestamp } }` |
| **ACK erreur** | `conversation_error` → `{ message, code }`                                    |

### `conversations:quickload`

|                |                                                                                                                      |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Entrée**     | `{ limit? }`                                                                                                         |
| **ACK succès** | `conversations:quick` → `{ conversations, hasMore, fromCache, totalUnreadMessages, unreadConversations, timestamp }` |
| **ACK erreur** | `conversations:error` → `{ error, code }`                                                                            |

### `conversations:fullload`

|                |                                                                        |
| -------------- | ---------------------------------------------------------------------- |
| **Entrée**     | `{ page?, limit?, cursor? }`                                           |
| **ACK succès** | `conversations:full` → `{ conversations, pagination, ..., timestamp }` |
| **ACK erreur** | `conversations:error` → `{ error, code }`                              |

### `conversation:load`

|                |                                                                  |
| -------------- | ---------------------------------------------------------------- |
| **Entrée**     | `{ conversationId }`                                             |
| **ACK succès** | `conversation:loaded` → `{ conversation, fromCache, timestamp }` |
| **ACK erreur** | `conversation:error` → `{ error, code }`                         |

### `joinConversation`

|                  |                                                              |
| ---------------- | ------------------------------------------------------------ |
| **Entrée**       | `{ conversationId }`                                         |
| **ACK succès**   | `conversation_joined` → `{ conversationId, timestamp }`      |
| **ACK erreur**   | `conversation_error` → `{ message, code }`                   |
| **Side effects** | Marque les messages comme lus. Met à jour la présence Redis. |

### `leaveConversation`

|                         |                                                                               |
| ----------------------- | ----------------------------------------------------------------------------- |
| **Entrée**              | `{ conversationId }`                                                          |
| **Broadcast**           | `user_left_conversation` → `{ userId, matricule, conversationId, timestamp }` |
| **⏭️ Émetteur exclu ?** | ✅ Oui — `socket.to(room).emit()` exclut nativement l'émetteur                |

### `leaveConversationPermanent`

|                         |                                                                                                         |
| ----------------------- | ------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ conversationId }`                                                                                    |
| **ACK succès**          | `conversation:left_permanent` → `{ success, conversationId, userId, remainingParticipants, timestamp }` |
| **ACK erreur**          | `conversation:error` → `{ error, code }`                                                                |
| **Broadcast**           | `participant:left` → `{ conversationId, userId, matricule, timestamp }`                                 |
| **⏭️ Émetteur exclu ?** | ✅ Oui — `socket.to(room).emit()` exclut nativement l'émetteur                                          |

---

## Groupes & Diffusion

### `createGroup`

|                    |                                                                                                                             |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ name, type?, members, groupId?, admins? }`                                                                               |
| **ACK succès**     | `group:created` → `{ success, group: { id, name, type, participants, createdBy, createdAt, participantCount }, timestamp }` |
| **ACK erreur**     | `group:error` → `{ error, code, details? }`                                                                                 |
| ~~Broadcast~~      | ~~`group:invitation`~~ — **supprimé**, distribution assurée par MDS ci-dessous                                              |
| 🔄 **MDS →**       | `conversation:created` → participants (⏭️ socket émetteur exclu)                                                            |
| **senderSocketId** | ✅ Propagé : chatHandler → CreateGroup → addToStream → MDS                                                                  |

### `createBroadcast`

|                    |                                                                                                                                                             |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ name, recipients, broadcastId?, admins? }`                                                                                                               |
| **ACK succès**     | `broadcast:created` → `{ success, broadcast: { id, name, type, participants, createdBy, createdAt, participantCount, adminIds, recipientIds }, timestamp }` |
| **ACK erreur**     | `broadcast:error` → `{ error, code, details? }`                                                                                                             |
| ~~Broadcast~~      | ~~`broadcast:admin_added`~~ + ~~`broadcast:subscription`~~ — **supprimés**, distribution assurée par MDS                                                    |
| 🔄 **MDS →**       | `conversation:created` → participants (⏭️ socket émetteur exclu)                                                                                            |
| **senderSocketId** | ✅ Propagé : chatHandler → CreateBroadcast → addToStream → MDS                                                                                              |

### `joinGroup`

|                               |                                                                                      |
| ----------------------------- | ------------------------------------------------------------------------------------ |
| **Entrée**                    | `{ conversationId, accept? }`                                                        |
| **ACK succès (accept=true)**  | `group:joined` → `{ success, conversationId, timestamp }`                            |
| **ACK succès (accept=false)** | `group:invitation_declined` → `{ conversationId, timestamp }`                        |
| **ACK erreur**                | `group:error` → `{ error, code }`                                                    |
| **Broadcast**                 | `group:member_joined` → `{ conversationId, user: { userId, matricule }, timestamp }` |
| **⏭️ Émetteur exclu ?**       | ✅ Oui — `socket.to(room).emit()` exclut nativement l'émetteur                       |

### `leaveGroup`

|                         |                                                                                    |
| ----------------------- | ---------------------------------------------------------------------------------- |
| **Entrée**              | `{ conversationId }`                                                               |
| **ACK succès**          | `group:left` → `{ success, conversationId, timestamp }`                            |
| **ACK erreur**          | `group:error` → `{ error, code }`                                                  |
| **Broadcast**           | `group:member_left` → `{ conversationId, user: { userId, matricule }, timestamp }` |
| **⏭️ Émetteur exclu ?** | ✅ Oui — `socket.to(room).emit()` exclut nativement l'émetteur                     |

### `getGroupInfo`

|                |                                                                                                                                                                      |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**     | `{ conversationId }`                                                                                                                                                 |
| **ACK succès** | `group:info` → `{ success, group: { id, name, type, participants, participantCount, createdBy, createdAt, lastMessage, settings, metadata }, fromCache, timestamp }` |
| **ACK erreur** | `group:error` → `{ error, code }`                                                                                                                                    |

---

## Appels (CALL / VIDEO_CALL)

### `initiateCall`

|                             |                                                                                                                                    |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**                  | `{ conversationId, receiverId, callType?, callId? }`                                                                               |
| **ACK succès**              | `call:initiated` → `{ success, callId, messageId, callType, conversationId, participants, timestamp }`                             |
| **ACK erreur**              | `call:error` → `{ error, code }`                                                                                                   |
| **Broadcast destinataires** | `call:incoming` → `{ callId, messageId, callType, conversationId, caller: { userId, matricule, nom, prenom, avatar }, timestamp }` |
| **⏭️ Émetteur exclu ?**     | ✅ Oui — `call:incoming` est envoyé uniquement aux destinataires, pas à l'appelant (qui a déjà l'ACK `call:initiated`)             |
| **Side effects**            | Crée un message de type `CALL` ou `VIDEO_CALL` via `SendMessage`.                                                                  |

### `answerCall`

|                    |                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ callId, messageId, conversationId? }`                                                                       |
| **ACK succès**     | `call:answered` → `{ success, callId, messageId, conversationId, answeredBy, answeredByMatricule, timestamp }` |
| **ACK erreur**     | `call:error` → `{ error, code }`                                                                               |
| ~~Broadcast~~      | ~~`call:answered`~~ — **supprimé**, distribution assurée par MDS ci-dessous                                    |
| 🔄 **MDS →**       | `call:statusUpdated` → participants (⏭️ socket émetteur exclu)                                                 |
| **senderSocketId** | ✅ Propagé : chatHandler → UpdateCallStatus → addToStream → MDS                                                |

### `declineCall`

|                    |                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ callId, messageId?, conversationId? }`                                                                      |
| **ACK succès**     | `call:declined` → `{ success, callId, messageId, conversationId, declinedBy, declinedByMatricule, timestamp }` |
| **ACK erreur**     | `call:error` → `{ error, code }`                                                                               |
| ~~Broadcast~~      | ~~`call:declined`~~ — **supprimé**, distribution assurée par MDS ci-dessous                                    |
| 🔄 **MDS →**       | `call:statusUpdated` → participants (⏭️ socket émetteur exclu)                                                 |
| **senderSocketId** | ✅ Propagé : chatHandler → UpdateCallStatus → addToStream → MDS                                                |

### `endCall`

|                    |                                                                                                                                     |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ callId, messageId?, conversationId?, reason? }`                                                                                  |
| **ACK succès**     | `call:ended` → `{ success, callId, messageId, conversationId, endedBy, endedByMatricule, reason, duration, timestamp }`             |
| **ACK erreur**     | `call:error` → `{ error, code }`                                                                                                    |
| ~~Broadcast~~      | ~~`call:ended`~~ — **supprimé**, distribution assurée par MDS ci-dessous                                                            |
| 🔄 **MDS →**       | `call:statusUpdated` → participants (⏭️ socket émetteur exclu)                                                                      |
| **senderSocketId** | ✅ Propagé : chatHandler → UpdateCallStatus → addToStream → MDS                                                                     |
| **Side effects**   | Calcule la durée depuis `startedAt`. Met à jour `call.status` → `ENDED`, `duration`, `endReason`. Met à jour le contenu du message. |

### `missedCall`

|                    |                                                                           |
| ------------------ | ------------------------------------------------------------------------- |
| **Entrée**         | `{ callId, messageId?, conversationId? }`                                 |
| ~~Broadcast~~      | ~~`call:missed`~~ — **supprimé**, distribution assurée par MDS ci-dessous |
| 🔄 **MDS →**       | `call:statusUpdated` → participants (⏭️ socket émetteur exclu)            |
| **senderSocketId** | ✅ Propagé : chatHandler → UpdateCallStatus → addToStream → MDS           |
| **Side effects**   | Met à jour `call.status` → `MISSED`, `endReason` → `no_answer`.           |

---

## Participants

### `addParticipant`

|                    |                                                                                                 |
| ------------------ | ----------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ conversationId, participantId }` _(string ou tableau)_                                       |
| **ACK succès**     | `participant:added` → `{ success, conversationId, participantIds, failed, addedBy, timestamp }` |
| **ACK erreur**     | `participant:error` → `{ error, code }`                                                         |
| ~~Broadcast~~      | ~~`participant:added` room broadcast~~ — **supprimé**, distribution assurée par MDS ci-dessous  |
| 🔄 **MDS →**       | `conversation:participant:added` → participants (⏭️ socket émetteur exclu)                      |
| **senderSocketId** | ✅ Propagé : chatHandler → AddParticipant → publishConversationEvent → MDS                      |

### `removeParticipant`

|                    |                                                                                                     |
| ------------------ | --------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ conversationId, participantId }` _(string ou tableau)_                                           |
| **ACK succès**     | `participant:removed` → `{ success, conversationId, participantIds, failed, removedBy, timestamp }` |
| **ACK erreur**     | `participant:error` → `{ error, code }`                                                             |
| ~~Broadcast~~      | ~~`participant:removed` room broadcast~~ — **supprimé**, distribution assurée par MDS ci-dessous    |
| 🔄 **MDS →**       | `conversation:participant:removed` → participants (⏭️ socket émetteur exclu)                        |
| **senderSocketId** | ✅ Propagé : chatHandler → RemoveParticipant → publishConversationEvent → MDS                       |

---

## Présence & Surveillance

### `getConversationOnlineUsers`

|                |                                                                                                                 |
| -------------- | --------------------------------------------------------------------------------------------------------------- |
| **Entrée**     | `{ conversationId }`                                                                                            |
| **ACK succès** | `conversation_online_users` → `{ conversationId, onlineUsers, totalUsers, users, userRole, currentUserStatus }` |
| **ACK erreur** | `conversation_users:error` → `{ error, code, details? }`                                                        |

### `getConversationsWithPresence`

|                |                                                                                        |
| -------------- | -------------------------------------------------------------------------------------- |
| **Entrée**     | _(aucun)_                                                                              |
| **ACK succès** | `conversations_with_presence` → `{ userId, conversations, count, summary, timestamp }` |
| **ACK erreur** | `conversations_presence:error` → `{ error, code, details? }`                           |

### `subscribeToPresence`

|                |                                                                                    |
| -------------- | ---------------------------------------------------------------------------------- |
| **Entrée**     | `{ conversationId }`                                                               |
| **ACK succès** | `presence:initial` → `{ conversationId, ...presenceStats, subscribed, timestamp }` |
| **ACK erreur** | `presence:error` → `{ error, code, details? }`                                     |

### `unsubscribeFromPresence`

|                |                                                           |
| -------------- | --------------------------------------------------------- |
| **Entrée**     | `{ conversationId }`                                      |
| **ACK succès** | `presence:unsubscribed` → `{ conversationId, timestamp }` |

### `getPresenceDashboard`

|                |                                                |
| -------------- | ---------------------------------------------- |
| **Entrée**     | _(aucun)_                                      |
| **ACK succès** | `presence_dashboard` → `{ ...dashboardData }`  |
| **ACK erreur** | `presence_dashboard:error` → `{ error, code }` |

---

## Rôles

### `setUserRole`

|                         |                                                                                                          |
| ----------------------- | -------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ conversationId, targetUserId, role }` _(member, moderator, admin)_                                    |
| **ACK succès**          | `role:updated` → `{ conversationId, targetUserId, role, updatedBy, timestamp }`                          |
| **ACK erreur**          | `role:error` → `{ error, code }`                                                                         |
| **Broadcast room**      | `user:role_changed` → `{ conversationId, userId, newRole, changedBy: { userId, matricule }, timestamp }` |
| **⏭️ Émetteur exclu ?** | ✅ Oui — `socket.to(room).emit()` exclut nativement l'émetteur                                           |

---

## Messages — Gestion (édition, suppression)

### `editMessage`

|                         |                                                                                                                        |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ messageId, newContent }`                                                                                            |
| **ACK succès**          | `message:edited` → `{ success, messageId, conversationId, userId, status: "EDITED", newContent, editedAt, timestamp }` |
| **ACK erreur**          | `message:error` → `{ error, code }`                                                                                    |
| ~~Broadcast~~           | ~~`message:edited` room broadcast~~ — **supprimé**, distribution assurée par MDS ci-dessous                            |
| 🔄 **MDS →**            | `message:status` → participants, status: `"EDITED"`, avec `newContent` (⏭️ socket émetteur exclu)                      |
| **senderSocketId**      | ✅ Propagé : chatHandler → UpdateMessageContent → publishEditedMessageToAllParticipants → publishMessageStatus → MDS   |
| **⏭️ Émetteur exclu ?** | ✅ Oui — ACK contient les mêmes données que MDS                                                                        |

### `deleteMessage`

|                         |                                                                                                                           |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ messageId, deleteType?, conversationId? }`                                                                             |
| **ACK succès**          | `message:deleted` → `{ success, messageId, conversationId, userId, status: "DELETED", deleteType, deletedAt, timestamp }` |
| **ACK erreur**          | `message:error` → `{ error, code }`                                                                                       |
| ~~Broadcast~~           | ~~`message:deleted` room broadcast~~ — **supprimé**, distribution assurée par MDS ci-dessous                              |
| 🔄 **MDS →**            | `message:status` → participants, status: `"DELETED"`, avec `deleteType` (⏭️ socket émetteur exclu)                        |
| **senderSocketId**      | ✅ Propagé : chatHandler → DeleteMessage → publishDeletedMessageToAllParticipants → publishMessageStatus → MDS            |
| **⏭️ Émetteur exclu ?** | ✅ Oui — ACK contient les mêmes données que MDS                                                                           |

---

## Fichiers

### `deleteFile`

|                    |                                                                                                                             |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**         | `{ fileId, physicalDelete? }`                                                                                               |
| **ACK succès**     | `file:deleted` → `{ success, fileId, deletedAt, physicalDelete, message, timestamp }`                                       |
| **ACK erreur**     | `file:error` → `{ error, code }`                                                                                            |
| 🔄 **MDS →**       | `file:event` → `{ fileId, event, fileName, fileSize, timestamp }`                                                           |
| **senderSocketId** | ⚠️ Non propagé — **OK par design** : l'événement `file:event` est envoyé uniquement à l'uploader (`userId`). Impact mineur. |

---

## Réactions

### `addReaction`

|                         |                                                                                                         |
| ----------------------- | ------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ messageId, emoji, conversationId? }`                                                                 |
| **ACK succès**          | `reaction:added` → `{ success, messageId, conversationId, userId, reaction, action: "add", timestamp }` |
| **ACK erreur**          | `reaction:error` → `{ error, code }`                                                                    |
| 🔄 **MDS →**            | `message:reaction` → `{ messageId, conversationId, userId, reaction, action: "add", timestamp }`        |
| **senderSocketId**      | ✅ Propagé directement dans le xAdd du chatHandler → MDS                                                |
| **⏭️ Émetteur exclu ?** | ✅ Oui — ACK contient les mêmes données que MDS                                                         |
| **Side effects**        | Sauvegarde la réaction en MongoDB (`message.reactions[]`).                                              |

### `removeReaction`

|                         |                                                                                                                  |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ messageId, conversationId? }`                                                                                 |
| **ACK succès**          | `reaction:removed` → `{ success, messageId, conversationId, userId, reaction: "", action: "remove", timestamp }` |
| **ACK erreur**          | `reaction:error` → `{ error, code }`                                                                             |
| 🔄 **MDS →**            | `message:reaction` → `{ messageId, conversationId, userId, reaction: "", action: "remove", timestamp }`          |
| **senderSocketId**      | ✅ Propagé directement dans le xAdd du chatHandler → MDS                                                         |
| **⏭️ Émetteur exclu ?** | ✅ Oui — ACK contient les mêmes données que MDS                                                                  |
| **Side effects**        | Supprime la réaction de l'utilisateur en MongoDB.                                                                |

---

## Réponses

### `replyToMessage`

|                         |                                                                                                                                                                             |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ messageId, content, conversationId? }`                                                                                                                                   |
| **ACK succès**          | `reply:sent` → `{ success, messageId, replyId, conversationId, userId, content, timestamp }`                                                                                |
| **ACK erreur**          | `reply:error` → `{ error, code }`                                                                                                                                           |
| 🔄 **MDS → (message)**  | Le message de réponse est un vrai `SendMessage` avec `replyTo` → publié dans `chat:stream:messages:*` → MDS livre `newMessage` / `message:group` (⏭️ socket émetteur exclu) |
| **senderSocketId**      | ✅ Propagé via SendMessage → ResilientMessageService → MDS                                                                                                                  |
| **⏭️ Émetteur exclu ?** | ✅ Oui — ACK `reply:sent` + message livré via MDS comme un message normal                                                                                                   |
| **Side effects**        | Délègue à `SendMessage` avec `replyTo` : save, WAL, stream, `updateLastMessage`, `unreadCount`. Le message porte `replyTo` (ObjectId du message parent).                    |

---

## Transfert

### `forwardMessage`

|                         |                                                                                                                                                                                     |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Entrée**              | `{ messageId, targetConversationIds }` — `targetConversationIds` : string ou string[] (max 10)                                                                                      |
| **ACK succès**          | `forward:sent` → `{ success, originalMessageId, forwarded[], errors[], count, userId, timestamp }`                                                                                  |
| **ACK erreur**          | `forward:error` → `{ error, code }` — codes : `AUTH_REQUIRED`, `MISSING_PARAMS`, `SERVICE_UNAVAILABLE`, `FORWARD_FAILED`                                                            |
| 🔄 **MDS → (message)**  | Chaque message transféré est un vrai `SendMessage` → publié dans `chat:stream:messages:private/group/channel` → MDS livre `newMessage` / `message:group` (⏭️ socket émetteur exclu) |
| **senderSocketId**      | ✅ Propagé via ForwardMessage → SendMessage → ResilientMessageService → MDS                                                                                                         |
| **⏭️ Émetteur exclu ?** | ✅ Oui — ACK `forward:sent` + chaque message livré via MDS comme un message normal                                                                                                  |
| **Side effects**        | Délègue à `SendMessage` par conversation cible : save, WAL, stream, `updateLastMessage`, `unreadCount`. Message porte `isForwarded: true`, `forwardedFrom`, `originalSenderId`.     |

**Détails `forwarded[]`** (un élément par conversation cible réussie) :

```json
{
  "messageId": "ObjectId",
  "conversationId": "ObjectId",
  "conversationType": "PRIVATE|GROUP|BROADCAST|CHANNEL",
  "content": "contenu copié",
  "type": "TEXT|IMAGE|...",
  "isForwarded": true,
  "originalMessageId": "ObjectId",
  "originalSenderId": "string",
  "timestamp": "ISO8601"
}
```

---

## Typing

> ⚠️ **Changement architectural (27 mars 2026)** : le typing n'est plus distribué par le `MessageDeliveryService` (MDS).
> Il est désormais géré **exclusivement** par le `TypingIndicatorService` (consumer dédié) qui apporte debounce, timeout et état en mémoire.

### `typing`

|                          |                                                                     |
| ------------------------ | ------------------------------------------------------------------- |
| **Entrée**               | `{ conversationId, event? }`                                        |
| **event values**         | `"typing:start"` (défaut), `"typing:refresh"`                       |
| ~~Broadcast~~            | ~~`userTyping`~~ — **supprimé**                                     |
| ~~MDS~~                  | ~~`typing:event`~~ — **supprimé du MDS**                            |
| 🔄 **TypingIndicator →** | `typing:indicator` → participants (sauf émetteur)                   |
| **Payload reçu**         | `{ conversationId, userId, status: "start"\|"refresh", timestamp }` |
| **Debounce serveur**     | 1s min entre chaque broadcast `refresh` du même utilisateur         |
| **Timeout auto**         | 10s — si pas de `refresh`, un `typing:stop` automatique est envoyé  |

### `stopTyping`

|                          |                                                         |
| ------------------------ | ------------------------------------------------------- |
| **Entrée**               | `{ conversationId }`                                    |
| ~~Broadcast~~            | ~~`userStoppedTyping`~~ — **supprimé**                  |
| ~~MDS~~                  | ~~`typing:event`~~ — **supprimé du MDS**                |
| 🔄 **TypingIndicator →** | `typing:indicator` → participants (sauf émetteur)       |
| **Payload reçu**         | `{ conversationId, userId, status: "stop", timestamp }` |

### Flux complet typing

```
Client emit "typing"/"stopTyping"
  → chatHandler publie dans Redis Stream "chat:stream:events:typing"
    → TypingIndicatorService (consumer group "typing-indicators", polling 50ms)
      → debounce 1s + timeout 10s + état mémoire
        → socket.emit("typing:indicator") aux participants
```

---

## Utilitaires (ping, heartbeat)

### `ping`

|         |        |
| ------- | ------ |
| **ACK** | `pong` |

### `heartbeat`

|         |                 |
| ------- | --------------- |
| **ACK** | `heartbeat_ack` |

---

## Récapitulatif — Émissions MessageDeliveryService (MDS)

Le `MessageDeliveryService` consomme les Redis Streams et **émet ces événements socket aux clients connectés** de manière asynchrone.

La colonne **⏭️ Exclusion** indique si le socket émetteur est exclu via `senderSocketId`.

| Événement socket émis                         | Stream source         | Destinataires                            | ⏭️ Exclusion           | Payload résumé                                                              |
| --------------------------------------------- | --------------------- | ---------------------------------------- | ---------------------- | --------------------------------------------------------------------------- |
| `newMessage`                                  | `private`             | Destinataire + autres appareils émetteur | ✅ socket émetteur     | `{ messageId, conversationId, senderId, receiverId, content, ... }`         |
| `message:group`                               | `group`               | Participants de la conversation          | ✅ socket émetteur     | `{ messageId, conversationId, senderId, senderName, content, ... }`         |
| `message:channel`                             | `channel`             | Participants du canal                    | ✅ socket émetteur     | `{ messageId, conversationId, senderId, senderName, content, ... }`         |
| `newMessage` _(SYSTEM)_                       | `group`               | Tous les participants                    | — (système)            | `{ ..., type: "SYSTEM", subType, ... }`                                     |
| `typing:indicator` _(TypingIndicatorService)_ | `typing` stream       | Participants (sauf émetteur)             | ✅ non livré au typeur | `{ conversationId, userId, status: "start"\|"refresh"\|"stop", timestamp }` |
| `message:status` _(DELIVERED)_                | `statusDelivered`     | Expéditeur original du message           | ⚠️ non (par design)    | `{ messageId, conversationId, userId, status, timestamp }`                  |
| `message:status` _(READ)_                     | `statusRead`          | Expéditeur original du message           | ⚠️ non (par design)    | `{ messageId, conversationId, userId, status, timestamp }`                  |
| `message:status` _(READ bulk)_                | `statusRead`          | Participants                             | ⚠️ non (par design)    | `{ isBulk: true, conversationId, userId, status, messageCount, ... }`       |
| `message:status` _(EDITED)_                   | `statusEdited`        | Tous les participants                    | ✅ socket émetteur     | `{ messageId, conversationId, userId, status, newContent, timestamp }`      |
| `message:status` _(DELETED)_                  | `statusDeleted`       | Tous les participants                    | ✅ socket émetteur     | `{ messageId, conversationId, userId, status, deleteType, timestamp }`      |
| `conversation:created`                        | `conversationCreated` | Participants de la nouvelle conversation | ✅ socket émetteur     | `{ conversationId, name, type, createdBy, participants, ... }`              |
| `conversation:updated`                        | `conversationUpdated` | Participants de la conversation          | ✅ socket émetteur     | `{ conversationId, name, updatedBy, changes, timestamp }`                   |
| `conversation:participant:added`              | `participantAdded`    | Participants (y compris le nouveau)      | ✅ socket émetteur     | `{ conversationId, participantId, participantName, addedBy, ... }`          |
| `conversation:participant:removed`            | `participantRemoved`  | Participants (y compris le retiré)       | ✅ socket émetteur     | `{ conversationId, participantId, participantName, removedBy, ... }`        |
| `conversation:deleted`                        | `conversationDeleted` | Participants de la conversation          | ✅ socket émetteur     | `{ conversationId, deletedBy, timestamp }`                                  |
| `call:statusUpdated`                          | `call`                | Tous les participants de l'appel         | ✅ socket émetteur     | `{ messageId, callId, conversationId, status, userId, ... }`                |
| `file:event`                                  | `files`               | Propriétaire du fichier                  | ⚠️ non (cible unique)  | `{ fileId, event, fileName, fileSize, timestamp }`                          |
| `message:reaction`                            | `reactions`           | Participants de la conversation          | ✅ socket émetteur     | `{ messageId, conversationId, userId, reaction, action, timestamp }`        |
| `message:reply`                               | `replies`             | Participants de la conversation          | ✅ socket émetteur     | `{ messageId, replyId, conversationId, userId, content, timestamp }`        |

---

## Architecture Multi-Device (`senderSocketId`)

### Principe

Chaque utilisateur peut être connecté sur **plusieurs appareils** simultanément. Le mécanisme `senderSocketId` garantit que :

1. **L'émetteur** reçoit un **ACK immédiat** via `socket.emit()` uniquement sur le socket qui a déclenché l'action
2. **Tous les autres participants** reçoivent via MDS (Redis Streams → Socket.IO)
3. **Les autres appareils de l'émetteur** reçoivent aussi via MDS (seul le `socket.id` exact est exclu, pas tout le `userId`)

### Chaîne de propagation

```
chatHandler                    → senderSocketId: socket.id
    ↓
Use Case (execute)             → accepte senderSocketId
    ↓
ResilientMessageService        → inclut senderSocketId dans les champs du stream Redis
    ↓
Redis Stream                   → senderSocketId stocké dans l'entrée
    ↓
MessageDeliveryService         → lit senderSocketId, exclut CE socket précis
    ↓
Socket.IO emit                 → tous les sockets SAUF le socket émetteur
```

### Statut par type d'événement

| Type d'événement                 | senderSocketId | Justification                                                                    |
| -------------------------------- | :------------: | -------------------------------------------------------------------------------- |
| Messages (privé/groupe/canal)    |       ✅       | Émetteur a l'ACK `message_sent`                                                  |
| Typing (start/stop)              |       ✅       | Pas besoin de se voir taper soi-même (TypingIndicatorService exclut le typeur)   |
| Status DELIVERED                 |     ⚠️ non     | Cible = expéditeur original (pas l'acteur) → tous ses appareils doivent recevoir |
| Status READ                      |     ⚠️ non     | Idem DELIVERED                                                                   |
| Status EDITED                    |       ✅       | Émetteur a l'ACK `message:edited`                                                |
| Status DELETED                   |       ✅       | Émetteur a l'ACK `message:deleted`                                               |
| Appels (answer/decline/end/miss) |       ✅       | Émetteur a l'ACK `call:answered/declined/ended`                                  |
| Conversation créée               |       ✅       | Émetteur a l'ACK `group:created` ou `broadcast:created`                          |
| Participant ajouté               |       ✅       | Émetteur a l'ACK `participant:added`                                             |
| Participant retiré               |       ✅       | Émetteur a l'ACK `participant:removed`                                           |
| Conversation mise à jour         |       ✅       | Préventif (pas de publisher actif encore)                                        |
| Conversation supprimée           |       ✅       | Préventif (pas de publisher actif encore)                                        |
| Fichier supprimé                 |     ⚠️ non     | Cible unique (uploader) — impact mineur                                          |
| Réactions                        |       ✅       | Émetteur a l'ACK `reaction:added/removed`                                        |
| Réponses (replies)               |       ✅       | Émetteur a l'ACK `reply:sent`                                                    |

### Broadcasts directs restants dans le chatHandler

Ces broadcasts **Room** (`socket.to().emit()`) ne passent PAS par MDS et restent en place car il n'y a **pas de stream Redis dédié** pour ces événements.

> **Note :** `socket.to(room).emit()` et `socket.broadcast.emit()` **excluent automatiquement** le socket émetteur — c'est le comportement natif de Socket.IO.

| Événement                | Contexte                     | ⏭️ Émetteur exclu ?                                  | Justification du maintien                          |
| ------------------------ | ---------------------------- | ---------------------------------------------------- | -------------------------------------------------- |
| `group:member_joined`    | `joinGroup`                  | ✅ Oui — `socket.to(room).emit()` exclut l'émetteur  | Notification ponctuelle room — pas de stream dédié |
| `group:member_left`      | `leaveGroup`                 | ✅ Oui — `socket.to(room).emit()` exclut l'émetteur  | Notification ponctuelle room — pas de stream dédié |
| `user:role_changed`      | `setUserRole`                | ✅ Oui — `socket.to(room).emit()` exclut l'émetteur  | Notification ponctuelle room — pas de stream dédié |
| `user_left_conversation` | `leaveConversation`          | ✅ Oui — `socket.to(room).emit()` exclut l'émetteur  | Notification ponctuelle room — pas de stream dédié |
| `participant:left`       | `leaveConversationPermanent` | ✅ Oui — `socket.to(room).emit()` exclut l'émetteur  | Notification ponctuelle room — pas de stream dédié |
| `user_disconnected`      | `disconnect`                 | ✅ Oui — `socket.broadcast.emit()` exclut l'émetteur | Notification ponctuelle globale                    |

### Broadcasts supprimés (total : 13)

| #   | Broadcast supprimé           | Handler d'origine   | Remplacé par                                |
| --- | ---------------------------- | ------------------- | ------------------------------------------- |
| 1   | `group:invitation`           | `createGroup`       | MDS → `conversation:created`                |
| 2   | `broadcast:admin_added`      | `createBroadcast`   | MDS → `conversation:created`                |
| 3   | `broadcast:subscription`     | `createBroadcast`   | MDS → `conversation:created`                |
| 4   | `call:answered` (room)       | `answerCall`        | MDS → `call:statusUpdated`                  |
| 5   | `call:declined` (room)       | `declineCall`       | MDS → `call:statusUpdated`                  |
| 6   | `call:ended` (room)          | `endCall`           | MDS → `call:statusUpdated`                  |
| 7   | `call:missed` (room)         | `missedCall`        | MDS → `call:statusUpdated`                  |
| 8   | `userTyping` (room)          | `typing`            | TypingIndicatorService → `typing:indicator` |
| 9   | `userStoppedTyping` (room)   | `stopTyping`        | TypingIndicatorService → `typing:indicator` |
| 10  | `message:edited` (room)      | `editMessage`       | MDS → `message:status` EDITED               |
| 11  | `message:deleted` (room)     | `deleteMessage`     | MDS → `message:status` DELETED              |
| 12  | `participant:added` (room)   | `addParticipant`    | MDS → `conversation:participant:added`      |
| 13  | `participant:removed` (room) | `removeParticipant` | MDS → `conversation:participant:removed`    |

### Résumé — Exclusion de l'émetteur (tous canaux confondus)

Ce tableau consolide **toutes** les émissions (MDS + broadcasts directs) et indique si l'émetteur est exclu des destinataires.

| Événement handler            | Canal de distribution                       | ⏭️ Émetteur exclu ?   | Mécanisme d'exclusion                                              |
| ---------------------------- | ------------------------------------------- | --------------------- | ------------------------------------------------------------------ |
| `sendMessage` (privé)        | MDS → `newMessage`                          | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `sendMessage` (groupe)       | MDS → `message:group`                       | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `sendMessage` (canal)        | MDS → `message:channel`                     | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `sendMessage` (système)      | MDS → `newMessage`                          | — N/A                 | Message système — pas d'émetteur humain                            |
| `typing` / `stopTyping`      | TypingIndicatorService → `typing:indicator` | ✅ Oui                | Le service exclut le typeur par userId (pas par socketId)          |
| `editMessage`                | MDS → `message:status` EDITED               | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `deleteMessage`              | MDS → `message:status` DELETED              | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `answerCall`                 | MDS → `call:statusUpdated`                  | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `declineCall`                | MDS → `call:statusUpdated`                  | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `endCall`                    | MDS → `call:statusUpdated`                  | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `missedCall`                 | MDS → `call:statusUpdated`                  | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `createGroup`                | MDS → `conversation:created`                | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `createBroadcast`            | MDS → `conversation:created`                | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `addParticipant`             | MDS → `participant:added`                   | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `removeParticipant`          | MDS → `participant:removed`                 | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `markMessageDelivered`       | MDS → `message:status` DELIVERED            | ⚠️ Non (par design)   | Cible = expéditeur original → doit recevoir sur tous ses appareils |
| `markMessageRead`            | MDS → `message:status` READ                 | ⚠️ Non (par design)   | Cible = expéditeur original → doit recevoir sur tous ses appareils |
| `deleteFile`                 | MDS → `file:event`                          | ⚠️ Non (cible unique) | Envoyé uniquement au propriétaire du fichier                       |
| `addReaction`                | MDS → `message:reaction`                    | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `removeReaction`             | MDS → `message:reaction`                    | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `replyToMessage`             | MDS → `message:reply`                       | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `forwardMessage`             | MDS → `newMessage` / `message:group`        | ✅ Oui                | `senderSocketId` dans Redis Stream → MDS skip ce socket            |
| `joinGroup`                  | Broadcast direct `group:member_joined`      | ✅ Oui                | `socket.to(room).emit()` — exclusion native Socket.IO              |
| `leaveGroup`                 | Broadcast direct `group:member_left`        | ✅ Oui                | `socket.to(room).emit()` — exclusion native Socket.IO              |
| `setUserRole`                | Broadcast direct `user:role_changed`        | ✅ Oui                | `socket.to(room).emit()` — exclusion native Socket.IO              |
| `leaveConversation`          | Broadcast direct `user_left_conversation`   | ✅ Oui                | `socket.to(room).emit()` — exclusion native Socket.IO              |
| `leaveConversationPermanent` | Broadcast direct `participant:left`         | ✅ Oui                | `socket.to(room).emit()` — exclusion native Socket.IO              |
| `disconnect`                 | Broadcast direct `user_disconnected`        | ✅ Oui                | `socket.broadcast.emit()` — exclusion native Socket.IO             |

**Résumé :**

- ✅ **25 événements** excluent correctement l'émetteur
- ⚠️ **3 événements** n'excluent pas l'émetteur — justifié par design (DELIVERED, READ, file:event)
- ❌ **0 événement** restant à corriger

---

### Problèmes connus restants

✅ **Aucun problème restant.** Tous les événements sont correctement filtrés par conversation et l'émetteur est exclu où nécessaire.

---

> **Changelog :**
>
> - **7 avril 2026** — Fix race condition `deleteMessage` → `markMessageRead`/`markMessageDelivered` : ajout garde-fou dans `MongoMessageRepository.updateSingleMessageStatus()` et `updateMessageStatus()` pour ne jamais écraser le status `"DELETED"` d'un message supprimé. Le `status` restait à `"READ"` malgré `isDeleted: true` + `deletedFor: "EVERYONE"` car un événement read/delivered arrivant après la suppression écrasait le status.
> - **7 avril 2026** — Correction `replyToMessage` : `SendMessage` accepte désormais `replyTo` et l'inclut dans le message sauvegardé (le champ était ignoré avant). Publication stream `chat:stream:events:replies` retirée du chatHandler (redondante, `SendMessage` publie déjà dans `chat:stream:messages:*`). ACK `reply:sent` corrigé (`result.message.id` au lieu de `result.messageId`).
> - **7 avril 2026** — Ajout du transfert de messages : événement `forwardMessage` avec ACK `forward:sent` / `forward:error`. Supporte le transfert vers 1 à 10 conversations simultanément. `ForwardMessage` délègue à `SendMessage` pour chaque conversation cible : chaque message transféré suit le flux standard (save → WAL → `chat:stream:messages:*` → MDS → `newMessage`). Le message porte `isForwarded: true`, `forwardedFrom` (ObjectId du message original), `originalSenderId`.
> - **27 mars 2026** — `markMessageRead` supporte le batch : accepte `{ conversationId, messageIds: [...] }` en plus du format unitaire `{ messageId }`. Le `unreadCount` est désormais **décrémenté** du nombre exact de messages lus au lieu d'être réinitialisé à 0.
> - **27 mars 2026** — Typing retiré du `MessageDeliveryService` : le stream `chat:stream:events:typing` est désormais consommé **uniquement** par le `TypingIndicatorService` (consumer group `typing-indicators`). Événement client renommé : `typing:event` → `typing:indicator` avec `status: "start"|"refresh"|"stop"` au lieu de `isTyping: bool`.
> - **25 mars 2026** — Alignement ACK ↔ MDS : les 5 ACK (editMessage, deleteMessage, addReaction, removeReaction, replyToMessage) contiennent désormais les mêmes champs que la livraison MDS. MDS EDITED inclut `newContent`, MDS DELETED inclut `deleteType`. Chaîne complète de propagation `deleteType` mise en place.
> - **25 mars 2026** — Correction `message:reaction` et `message:reply` : ajout handlers `addReaction`, `removeReaction`, `replyToMessage` dans chatHandler. MDS corrigé : filtrage par conversation + exclusion `senderSocketId`. Total : 24/27 événements avec exclusion émetteur, 3 justifiés par design, 0 à corriger.
> - **25 mars 2026** — Audit complet `senderSocketId` sur tous les types d'événements. Suppression de 4 broadcasts résiduels supplémentaires (editMessage, deleteMessage, addParticipant, removeParticipant). Ajout `senderSocketId` pour `createBroadcast`. Total : 13 broadcasts supprimés.
> - **24 mars 2026** — Suppression des 9 premiers broadcasts redondants (group:invitation, broadcast:admin_added, broadcast:subscription, call:answered/declined/ended/missed, userTyping, userStoppedTyping).
