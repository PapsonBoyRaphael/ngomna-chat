# REFERENCE TECHNIQUE

Ce document est une fusion automatisée des fichiers de documentation suivants :



---
## Fichier d'origine : `SOCKET_EVENTS_REFERENCE.md`

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



---
## Fichier d'origine : `REDIS_KEYS_CONVENTION.md`

# Convention de Nommage des Clés Redis - Chat Application

## 📋 Structure Hiérarchique

Toutes les clés Redis sont organisées selon la structure suivante:

```
chat/
├── cache/     (données en cache, présence, utilisateurs, rooms)
└── stream/    (tous les Redis Streams)
```

---

## 🔑 Clés Cache (`chat:cache:*`)

### Présence et Utilisateurs en Ligne

| Clé                                    | Description                            | Exemple                               | TTL      |
| -------------------------------------- | -------------------------------------- | ------------------------------------- | -------- |
| `chat:cache:presence:{userId}`         | État de présence (online/idle/offline) | `chat:cache:presence:570479H`         | 5 min    |
| `chat:cache:user_data:{userId}`        | Hash avec données utilisateur          | `chat:cache:user_data:570479H`        | 5 min    |
| `chat:cache:user_sockets:{socketId}`   | Mappe socket → userId                  | `chat:cache:user_sockets:abc123`      | 5 min    |
| `chat:cache:user_sockets_set:{userId}` | Set des sockets d'un utilisateur       | `chat:cache:user_sockets_set:570479H` | 1h       |
| `chat:cache:last_seen:{userId}`        | Dernier vu hors ligne (status, time)   | `chat:cache:last_seen:570479H`        | 30 jours |

### Rooms (Conversations)

| Clé                                         | Description                            | Type   | Exemple                                     |
| ------------------------------------------- | -------------------------------------- | ------ | ------------------------------------------- |
| `chat:cache:rooms:{roomName}`               | Métadonnées de la room                 | Hash   | `chat:cache:rooms:conv_507d0f`              |
| `chat:cache:room_users:{roomName}`          | Set des userIds dans la room           | Set    | `chat:cache:room_users:conv_507d0f`         |
| `chat:cache:user_rooms:{userId}`            | Set des rooms d'un utilisateur         | Set    | `chat:cache:user_rooms:570479H`             |
| `chat:cache:room_data:{roomName}:{userId}`  | Données utilisateur dans la room       | Hash   | `chat:cache:room_data:conv_507d0f:570479H`  |
| `chat:cache:room_state:{roomName}`          | État de la room (active/idle/archived) | String | `chat:cache:room_state:conv_507d0f`         |
| `chat:cache:room_roles:{roomName}:{userId}` | Rôle de l'utilisateur dans la room     | String | `chat:cache:room_roles:conv_507d0f:570479H` |
| `chat:cache:room_peak:{roomName}`           | Pic d'utilisateurs online              | String | `chat:cache:room_peak:conv_507d0f`          |

---

## 📊 Streams (`chat:stream:*`)

### Streams Techniques (Infrastructure)

| Stream                 | Description                          | Max Len |
| ---------------------- | ------------------------------------ | ------- |
| `chat:stream:wal`      | Write-Ahead Log pour résilience      | 10000   |
| `chat:stream:retry`    | Queue de retry pour messages échoués | 5000    |
| `chat:stream:dlq`      | Dead Letter Queue                    | 1000    |
| `chat:stream:fallback` | Fallback storage                     | 5000    |
| `chat:stream:metrics`  | Métriques et statistiques            | 10000   |

### Streams Fonctionnels (Messages)

#### Messages par Type

| Stream                         | Description        | Max Len |
| ------------------------------ | ------------------ | ------- |
| `chat:stream:messages:private` | Messages privés    | 10000   |
| `chat:stream:messages:group`   | Messages de groupe | 20000   |
| `chat:stream:messages:channel` | Messages de canal  | 20000   |

#### États des Messages

| Stream                         | Description        | Max Len |
| ------------------------------ | ------------------ | ------- |
| `chat:stream:status:delivered` | Messages livrés    | 5000    |
| `chat:stream:status:read`      | Messages lus       | 5000    |
| `chat:stream:status:edited`    | Messages édités    | 2000    |
| `chat:stream:status:deleted`   | Messages supprimés | 2000    |

#### Interactions

| Stream                         | Description            | TTL | Max Len |
| ------------------------------ | ---------------------- | --- | ------- |
| `chat:stream:events:typing`    | Indicateurs de saisie  | 60s | 2000    |
| `chat:stream:events:reactions` | Réactions aux messages | -   | 5000    |
| `chat:stream:events:replies`   | Réponses aux messages  | -   | 5000    |

### Streams Événementiels (Métier)

#### Conversations

| Stream                                                 | Description                             | Max Len |
| ------------------------------------------------------ | --------------------------------------- | ------- |
| `chat:stream:events:conversations`                     | Créations/suppressions de conversations | 5000    |
| `chat:stream:events:conversation:created`              | Conversation créée                      | 2000    |
| `chat:stream:events:conversation:updated`              | Conversation mise à jour                | 2000    |
| `chat:stream:events:conversation:participants:added`   | Participant ajouté                      | 2000    |
| `chat:stream:events:conversation:participants:removed` | Participant retiré                      | 2000    |
| `chat:stream:events:conversation:deleted`              | Conversation supprimée                  | 1000    |

#### Autres Événements

| Stream                             | Description           | Max Len |
| ---------------------------------- | --------------------- | ------- |
| `chat:stream:events:files`         | Événements fichier    | 5000    |
| `chat:stream:events:notifications` | Notifications système | 2000    |
| `chat:stream:events:analytics`     | Données analytiques   | 10000   |

---

## 🔄 Pub/Sub (Abonnement aux Expirations)

```
__keyevent@0__:expired
```

Utilisé pour détecter l'expiration des clés (présence, TTL utilisateurs, etc.)

---

## 📝 Schéma des Données

### User Data Hash

```javascript
{
  userId: "570479H",
  socketId: "socket-abc123",
  matricule: "MAT001",
  status: "online",
  lastActivity: "2025-02-08T12:30:00.000Z",
  connectedAt: "2025-02-08T12:00:00.000Z"
}
```

### Last Seen (Offline)

```javascript
{
  lastActivity: "2025-02-08T12:00:00.000Z",
  status: "offline",
  matricule: "MAT001",
  disconnectedAt: "2025-02-08T12:00:00.000Z"
}
```

### Room Data Hash

```javascript
{
  userId: "570479H",
  matricule: "MAT001",
  joinedAt: "2025-02-08T12:30:00.000Z",
  lastActivity: "2025-02-08T12:30:00.000Z",
  conversationId: "507d0f"
}
```

---

## 🎯 Migration depuis l'Ancienne Convention

| Ancien               | Nouveau                         |
| -------------------- | ------------------------------- |
| `presence:*`         | `chat:cache:presence:*`         |
| `user_data:*`        | `chat:cache:user_data:*`        |
| `user_sockets:*`     | `chat:cache:user_sockets:*`     |
| `user_sockets_set:*` | `chat:cache:user_sockets_set:*` |
| `last_seen:*`        | `chat:cache:last_seen:*`        |
| `rooms:*`            | `chat:cache:rooms:*`            |
| `room_users:*`       | `chat:cache:room_users:*`       |
| `user_rooms:*`       | `chat:cache:user_rooms:*`       |
| `room_data:*`        | `chat:cache:room_data:*`        |
| `room_state:*`       | `chat:cache:room_state:*`       |
| `stream:*`           | `chat:stream:*`                 |
| `stream:messages:*`  | `chat:stream:messages:*`        |
| `stream:status:*`    | `chat:stream:status:*`          |
| `stream:events:*`    | `chat:stream:events:*`          |

---

## 🛠️ Code d'Utilisation

### OnlineUserManager

```javascript
// Préfixes configurables mais par défaut:
this.presencePrefix = "chat:cache:presence";
this.userDataPrefix = "chat:cache:user_data";
this.userSocketPrefix = "chat:cache:user_sockets";
this.userSocketsSetPrefix = "chat:cache:user_sockets_set";

// Utilisation
await this.redis.set(`${this.presencePrefix}:${userId}`, "online");
```

### RoomManager

```javascript
// Préfixes configurables mais par défaut:
this.roomPrefix = "chat:cache:rooms";
this.roomUsersPrefix = "chat:cache:room_users";
this.userRoomsPrefix = "chat:cache:user_rooms";
this.roomDataPrefix = "chat:cache:room_data";
this.roomStatePrefix = "chat:cache:room_state";

// Utilisation
await this.redis.sAdd(`${this.roomUsersPrefix}:${roomName}`, userId);
```

### StreamManager

```javascript
// Tous les streams sont définis dans la classe
this.STREAMS = {
  WAL: "chat:stream:wal",
  RETRY: "chat:stream:retry",
  // ...
};

this.MESSAGE_STREAMS = {
  PRIVATE: "chat:stream:messages:private",
  // ...
};

this.EVENT_STREAMS = {
  CONVERSATIONS: "chat:stream:events:conversations",
  // ...
};
```

---

## ✅ Checkpoint

- [x] Tous les streams renommés vers `chat:stream:*`
- [x] Toutes les clés de cache renommées vers `chat:cache:*`
- [x] Préfixes configurables via options dans constructeurs
- [x] ResilientMessageService patterns mis à jour
- [x] Documentation complétée

---

## 📌 Notes

1. **Cohérence**: Tous les préfixes sont définis dans les constructeurs des managers
2. **Flexibilité**: Les préfixes peuvent être surchargés via les options du constructeur
3. **Namespacing**: Structure claire avec `chat:` comme racine pour toutes les données de chat
4. **TTL**: Respecte les durées de vie configurées pour chaque type de donnée
5. **Pub/Sub**: Utilise les keyspace notifications de Redis pour les expirations



---
## Fichier d'origine : `REDIS_DOCUMENTATION.md`

# 🔴 Documentation Shared Redis - Module Centralisé

## 📋 Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Initialisation](#initialisation)
- [RedisFactory](#redisfactory)
- [RedisManager](#redismanager)
- [CacheService](#cacheservice)
- [OnlineUserManager](#onlineusemanager)
- [RoomManager](#roommanager)
- [UnreadMessageManager](#unreadmessagemanager)
- [Configuration](#configuration)
- [Patterns d'utilisation](#patterns-dutilisation)
- [Monitoring & Métriques](#monitoring--métriques)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Vue d'ensemble

Le module `shared/redis` centralise **TOUS** les accès Redis du projet:

```
┌─────────────────────────────────────────┐
│   chat-file-service                     │
│   auth-service                          │
│   gateway                               │
└────────────────────┬────────────────────┘
                     │ (import depuis shared)
                     ↓
        ┌────────────────────────┐
        │   shared/redis         │
        │  (Module Centralisé)   │
        └─────────┬──────────────┘
                  │
        ┌─────────┴────────────────────────┐
        │                                  │
    ┌───▼──────┐               ┌───────────▼──┐
    │ RedisFactory │           │ RedisManager │
    │ (Connexions) │           │ (Singleton)  │
    └──────────┘               └──────────────┘
                               │
        ┌──────────────────────┼──────────────────┬──────────────┐
        │                      │                  │              │
    ┌───▼────────┐    ┌────────▼──────┐   ┌──────▼─────┐   ┌───▼──────────┐
    │CacheService│    │OnlineUser     │   │RoomManager │   │UnreadMessage │
    │            │    │Manager        │   │            │   │Manager       │
    └────────────┘    └───────────────┘   └────────────┘   └──────────────┘
```

### Localisation

```
shared/
  redis/
    index.js                   # Export centralisé
    redisConfig.js            # Legacy wrapper
    RedisFactory.js           # ✅ SEUL avec require("redis")
    RedisManager.js           # Singleton principal
    managers/
      CacheService.js         # Cache Redis
      OnlineUserManager.js    # Utilisateurs online
      RoomManager.js          # Rooms/salles
      UnreadMessageManager.js # Messages non lus
    workers/                  # Workers de résilience
```

### Principe clé

✅ **Un seul endroit avec require("redis")**

- RedisFactory.js = SEUL fichier avec `require("redis")`
- Tous les autres fichiers utilisent RedisManager/RedisFactory
- Injection de dépendances centralisée

---

## 🏗️ Architecture

### Pattern: Singleton + Factory + Managers

```javascript
// 1. RedisFactory crée les clients Redis
const factory = new RedisFactory("service-name");
const client = await factory.getClient("main");

// 2. RedisManager est un Singleton global
const manager = new RedisManager();
await manager.connect();

// 3. Les Managers utilisent RedisManager
const cache = new CacheService();
await cache.initialize(RedisManager);
await cache.set("key", "value");
```

### Clients Redis

| Type       | Usage                         | Nbr instances |
| ---------- | ----------------------------- | ------------- |
| **main**   | Opérations CRUD, GET/SET      | 1             |
| **pub**    | Publisher Pub/Sub             | 1             |
| **sub**    | Subscriber Pub/Sub            | 1             |
| **stream** | Stream commands (XREAD, XADD) | 1             |
| **cache**  | Cache hit/miss optimized      | 1             |

### Intégration avec résilience

```
RedisManager
    ├─ StreamManager
    │   └─ Write-Ahead Log (WAL)
    │   └─ Fallback storage
    └─ CircuitBreaker
        └─ Fail-safe pattern
```

---

## 🚀 Initialisation

### Méthode recommandée (via RedisFactory)

```javascript
const { RedisFactory, RedisService } = require("shared/redis");

// 1. Créer une instance de service
const redisService = RedisFactory.createService("chat-service");

// 2. Connecter tous les clients
await redisService.connect();

// 3. Accéder aux clients
const mainClient = redisService.getMainClient();
const pubClient = redisService.getPubClient();
const subClient = redisService.getSubClient();
```

### Méthode legacy (via redisConfig)

```javascript
const redisConfig = require("shared/redis").redisConfig;

await redisConfig.connect();
const client = redisConfig.getClient();
```

### Initialiser avec Managers

```javascript
const {
  RedisManager,
  CacheService,
  OnlineUserManager,
} = require("shared/redis");

// 1. Connecter RedisManager (Singleton)
await RedisManager.connect();

// 2. Initialiser les managers
const cache = new CacheService();
await cache.initialize(RedisManager);

const onlineUsers = new OnlineUserManager();
await onlineUsers.initialize(RedisManager);

// Maintenant ready pour utilisation
await cache.set("key", "value");
```

---

## 🏭 RedisFactory

**Rôle**: Créer et gérer les clients Redis

### Fichier

`shared/redis/RedisFactory.js`

### Classe: RedisService

```javascript
class RedisService {
  constructor(serviceName, options = {}) {
    // Configuration par service
    this.serviceName = serviceName;    // "chat-service", "auth-service"
    this.config = { ...DEFAULT_CONFIG, ...options };
    this.clients = new Map();          // Stocke tous les clients
    this.isConnected = false;
    this.metrics = { ... };            // Tracking
  }
}
```

### Méthodes clés

#### `async getClient(type)`

Obtenir ou créer un client par type.

```javascript
const service = RedisFactory.createService("chat");
const mainClient = await service.getClient("main"); // Crée + connecte
const pubClient = await service.getClient("pub"); // 2e client
const subClient = await service.getClient("sub"); // 3e client
```

#### `getMainClient()`

Accès direct au client principal.

```javascript
const client = service.getMainClient(); // Synchrone, pas d'await
if (client) {
  const value = await client.get("key");
}
```

#### `getPubClient() / getSubClient()`

Accès pub/sub.

```javascript
const pub = service.getPubClient();
const sub = service.getSubClient();

await pub.publish("channel", "message");
await sub.subscribe("channel", (message) => {
  console.log("Reçu:", message);
});
```

#### `async connect()`

Connecter tous les clients.

```javascript
const service = new RedisService("my-service");
await service.connect(); // Crée et connecte main, pub, sub, stream, cache
```

#### `async disconnect()`

Fermer tous les clients.

```javascript
await service.disconnect();
```

#### `async getHealthStatus()`

Vérifier la santé de la connexion.

```javascript
const status = await service.getHealthStatus();
// "OK" ou "Disconnected" ou message d'erreur
```

### Configuration

```javascript
const DEFAULT_CONFIG = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  db: parseInt(process.env.REDIS_DB) || 0,
  family: parseInt(process.env.REDIS_FAMILY) || 4,
  connectTimeout: parseInt(process.env.REDIS_CONNECTION_TIMEOUT) || 5000,
  maxRetriesPerRequest: 3,
  retryDelayOnFailover: 100,
};
```

### Métriques

```javascript
service.metrics = {
  clientsCreated: 0, // Nbr clients créés
  reconnections: 0, // Nbr reconnexions
  errors: 0, // Nbr erreurs
  lastConnectedAt: Date, // Dernière connexion réussie
  lastErrorAt: Date, // Dernière erreur
};
```

---

## 👑 RedisManager (Singleton)

**Rôle**: Gestionnaire centralisé principal du projet

### Fichier

`shared/redis/RedisManager.js`

### Instance Singleton

```javascript
// RedisManager est un Singleton (une seule instance dans le projet)
const RedisManager = require("shared/redis").RedisManager;

// À chaque appel, même instance
const mgr1 = new RedisManager();
const mgr2 = new RedisManager();
console.log(mgr1 === mgr2); // true ✅
```

### Clients gérés

```javascript
RedisManager.clients = {
  main: RedisClient, // Opérations CRUD
  pub: RedisClient, // Publisher
  sub: RedisClient, // Subscriber
  stream: RedisClient, // Streams
  cache: RedisClient, // Cache optimized
};
```

### Intégration résilience

```javascript
RedisManager.streamManager = StreamManager; // WAL + Fallback
RedisManager.circuitBreaker = CircuitBreaker; // Fail-safe
```

### Méthodes clés

#### `async connect()`

Connecter tous les clients et components de résilience.

```javascript
const manager = new RedisManager();
await manager.connect();

// Après: tous les clients connectés
// Streams prêts
// CircuitBreaker activé
```

#### `getMainClient()`

Accès au client principal.

```javascript
const client = manager.getMainClient();
const value = await client.get("key");
```

#### `getPubClient() / getSubClient()`

Accès Pub/Sub.

```javascript
const pub = manager.getPubClient();
const sub = manager.getSubClient();
```

#### `getStreamClient()`

Accès Streams.

```javascript
const stream = manager.getStreamClient();
await stream.xAdd("mystream", "*", "field", "value");
```

#### `getCacheClient()`

Accès cache optimisé.

```javascript
const cache = manager.getCacheClient();
await cache.get("key");
```

#### `async disconnect()`

Fermer tous les clients.

```javascript
await manager.disconnect();
```

#### `async getHealthStatus()`

Vérifier santé globale.

```javascript
const status = await manager.getHealthStatus();
console.log(status); // "OK", "DEGRADED", ou "Disconnected"
```

### Métriques

```javascript
manager.metrics = {
  connectionsCreated: 0,
  reconnections: 0,
  errors: 0,
  lastConnectedAt: Date,
  lastErrorAt: Date,
};
```

---

## 💾 CacheService

**Rôle**: Cache général Redis avec TTL et stratégies

### Fichier

`shared/redis/managers/CacheService.js`

### Initialisation

```javascript
const { CacheService, RedisManager } = require("shared/redis");

// Option 1: Via RedisManager
const cache = new CacheService({
  defaultTTL: 3600, // 1 heure
  keyPrefix: "chat", // Préfixe clés
  maxScanCount: 100,
});
await cache.initialize(RedisManager);

// Option 2: Avec client direct (compatibilité)
const cache = new CacheService();
const client = await redisService.getClient("cache");
cache.initializeWithClient(client);
```

### Opérations de base

#### `async set(key, value, ttl)`

Ajouter une valeur.

```javascript
// Basique
await cache.set("user:123", { name: "Alice", dept: "IT" });

// Avec TTL personnalisé
await cache.set("session:abc", tokenData, 1800); // 30 min

// JSON automatique
await cache.set("config", { debug: true, workers: 5 });
```

#### `async get(key)`

Récupérer une valeur.

```javascript
const user = await cache.get("user:123");
// Retourne l'objet désérialisé

const missing = await cache.get("nonexistent");
// Retourne null
```

#### `async del(key)`

Supprimer une clé.

```javascript
await cache.del("user:123");
```

#### `async exists(key)`

Vérifier l'existence.

```javascript
const found = await cache.exists("user:123");
// true ou false
```

#### `async renewTTL(key, ttl)`

Renouveler la durée de vie.

```javascript
// L'utilisateur a utilisé le cache récemment
// Garder les données un peu plus longtemps
await cache.renewTTL("user:123", 3600);
```

#### `async setMultiple(entries, ttl)`

Ajouter plusieurs clés.

```javascript
await cache.setMultiple(
  [
    { key: "user:1", value: userData1 },
    { key: "user:2", value: userData2 },
    { key: "user:3", value: userData3 },
  ],
  3600
);
```

#### `async getMultiple(keys)`

Récupérer plusieurs clés.

```javascript
const results = await cache.getMultiple(["user:1", "user:2", "user:3"]);
// [{key, value}, {key, value}, ...]
```

#### `async deletePattern(pattern)`

Supprimer par pattern.

```javascript
await cache.deletePattern("user:*"); // Toutes les clés user
await cache.deletePattern("session:*"); // Tous les sessions
```

#### `async flush()`

Vider tout le cache.

```javascript
await cache.flush();
```

#### `async keys(pattern)`

Lister les clés.

```javascript
const keys = await cache.keys("user:*");
console.log(keys); // ["user:1", "user:2", "user:3"]
```

### Exemples réels

**Cachage utilisateur**

```javascript
// Récupérer ou créer
let user = await cache.get("user:123");
if (!user) {
  user = await UserCacheService.getUserProfile(123);
  await cache.set("user:123", user, 86400); // 24h
}
```

**Cachage conversation**

```javascript
const convId = "507f1f77bcf86cd799439011";
let conv = await cache.get(`conv:${convId}`);
if (!conv) {
  conv = await ConversationRepository.findById(convId);
  await cache.set(`conv:${convId}`, conv, 3600); // 1h
}
```

---

## 👥 OnlineUserManager

**Rôle**: Tracker des utilisateurs en ligne en temps réel

### Fichier

`shared/redis/managers/OnlineUserManager.js`

### Initialisation

```javascript
const { OnlineUserManager, RedisManager } = require("shared/redis");

const onlineUsers = new OnlineUserManager(io, {
  presencePrefix: "presence",
  userDataPrefix: "user_data",
  userSocketPrefix: "user_sockets",
  defaultTTL: 300, // 5 min
  idleTTL: 3600, // 1 heure
});

await onlineUsers.initialize(RedisManager);
```

### Opérations clés

#### `async setUserOnline(userId, userData)`

Marquer utilisateur online.

```javascript
await onlineUsers.setUserOnline("507f1f77bcf86cd799439011", {
  socketId: "socket-123",
  matricule: "USER001",
  connectedAt: new Date(),
  lastActivity: new Date(),
});
```

#### `async setUserOffline(userId)`

Marquer utilisateur offline.

```javascript
await onlineUsers.setUserOffline("507f1f77bcf86cd799439011");
```

#### `async getOnlineUsers()`

Lister tous les users online.

```javascript
const users = await onlineUsers.getOnlineUsers();
// [{userId, socketId, matricule, status, connectedAt}]
```

#### `async isUserOnline(userId)`

Vérifier si online.

```javascript
const isOnline = await onlineUsers.isUserOnline("507f1f77bcf86cd799439011");
// true ou false
```

#### `async updateLastActivity(userId)`

Renouveler TTL (marquer utilisé).

```javascript
// Chaque action (message, typing, etc.)
await onlineUsers.updateLastActivity("507f1f77bcf86cd799439011");
```

#### `async getOnlineCount()`

Nombre total d'users online.

```javascript
const count = await onlineUsers.getOnlineCount();
console.log(`${count} utilisateurs en ligne`);
```

#### `async getPresenceStats()`

Statistiques complètes de présence.

```javascript
const stats = await onlineUsers.getPresenceStats();
// {
//   totalOnlineUsers: 150,
//   newConnectionsLastHour: 30,
//   averageSessionDuration: 1800,
//   peakOnlineUsers: 200,
//   statusDistribution: {online, away, idle}
// }
```

### Durée de vie

```
User online → 5 min TTL
  ↓ (user actif)
Renew TTL → 5 min additionnelles
  ↓ (inactif > 5 min)
Expire automatiquement → Offline
  ↓ (ou après 1 heure idle)
Archive → Historique présence
```

---

## 🎪 RoomManager

**Rôle**: Gérer les rooms/salles de conversation avec présence

### Fichier

`shared/redis/managers/RoomManager.js`

### Initialisation

```javascript
const {
  RoomManager,
  OnlineUserManager,
  RedisManager,
} = require("shared/redis");

const rooms = new RoomManager(io, onlineUserManager, {
  roomPrefix: "rooms",
  roomUsersPrefix: "room_users",
  userRoomsPrefix: "user_rooms",
  defaultRoomTTL: 3600,
  idleRoomTTL: 7200,
  archivedRoomTTL: 86400,
});

await rooms.initialize(RedisManager);
```

### Opérations clés

#### `async createRoom(roomId, data)`

Créer une room.

```javascript
await rooms.createRoom("conv_507f...", {
  name: "Dev Team",
  type: "GROUP",
  createdAt: new Date(),
  metadata: { topic: "Développement" },
});
```

#### `async getRoomInfo(roomId)`

Récupérer infos de la room.

```javascript
const roomData = await rooms.getRoomInfo("conv_507f...");
// {id, name, type, createdAt, userCount, metadata}
```

#### `async joinRoom(roomId, userId)`

Ajouter utilisateur à la room.

```javascript
await rooms.joinRoom("conv_507f...", "user123");

// Tracking automatique
// ├─ room_users:conv_507f... = [user123, user456]
// └─ user_rooms:user123 = [conv_507f..., conv_abc...]
```

#### `async leaveRoom(roomId, userId)`

Retirer utilisateur de la room.

```javascript
await rooms.leaveRoom("conv_507f...", "user123");
```

#### `async getRoomUsers(roomId)`

Lister les users dans une room.

```javascript
const users = await rooms.getRoomUsers("conv_507f...");
// [userId1, userId2, userId3, ...]
```

#### `async getRoomOnlineUsers(roomId)`

Lister les users online dans une room.

```javascript
const onlineUsers = await rooms.getRoomOnlineUsers("conv_507f...");
// [userId1, userId2]

const stats = await rooms.getRoomOnlineUsersCount("conv_507f...");
// {onlineCount: 2, totalCount: 5}
```

#### `async getUserRoleInRoom(roomId, userId)`

Récupérer le rôle d'un user.

```javascript
const role = await rooms.getUserRoleInRoom("conv_507f...", "user123");
// "admin", "moderator", ou "member"
```

#### `async setUserRoleInRoom(roomId, userId, role)`

Définir le rôle d'un user.

```javascript
await rooms.setUserRoleInRoom("conv_507f...", "user123", "moderator");
```

#### `async getUserRooms(userId)`

Lister les rooms d'un user.

```javascript
const myRooms = await rooms.getUserRooms("user123");
// ["conv_507f...", "conv_abc...", "conv_def..."]
```

#### `async getRoomPeakMetrics(roomId)`

Métriques de pic pour une room.

```javascript
const peak = await rooms.getRoomPeakMetrics("conv_507f...");
// {
//   peakUsersCount: 5,
//   peakTime: Date,
//   averageActiveUsers: 3
// }
```

#### `async getRoomPresenceStats(roomId)`

Stats de présence.

```javascript
const stats = await rooms.getRoomPresenceStats("conv_507f...");
// {
//   roomId, onlineUsers, totalUsers,
//   users: [{userId, status, lastActivity}],
//   averageSessionDuration
// }
```

### Structures Redis

```redis
rooms:conv_507f...
  ├─ id: "conv_507f..."
  ├─ name: "Dev Team"
  ├─ type: "GROUP"
  └─ userCount: 5

room_users:conv_507f...
  └─ [user1, user2, user3, user4, user5]

user_rooms:user1
  └─ [conv_507f..., conv_abc...]

room_roles:conv_507f...
  ├─ user1: "admin"
  ├─ user2: "moderator"
  └─ user3: "member"
```

---

## 📬 UnreadMessageManager

**Rôle**: Gérer les compteurs de messages non lus

### Fichier

`shared/redis/managers/UnreadMessageManager.js`

### Initialisation

```javascript
const { UnreadMessageManager, RedisManager } = require("shared/redis");

const unread = new UnreadMessageManager({
  keyPrefix: "unread",
  userUnreadPrefix: "user_unread",
  conversationUnreadPrefix: "conversation_unread",
  defaultTTL: 3 * 24 * 3600, // 3 jours
});

await unread.initialize(RedisManager);

// Injecter les callbacks de recalcul
unread.setRecalculateFunction(async (convId, userId) => {
  return await MessageRepository.countUnread(convId, userId);
});

unread.setRecalculateTotalFunction(async (userId) => {
  return await MessageRepository.countUserTotalUnread(userId);
});
```

### Opérations clés

#### `async incrementUnread(conversationId, userId, count)`

Incrémenter compteur non lu.

```javascript
// Nouveau message arrives dans une conversation
await unread.incrementUnread("conv_507f...", "user123", 1);
```

#### `async decrementUnread(conversationId, userId, count)`

Décrémenter compteur.

```javascript
// User lit les messages
await unread.decrementUnread("conv_507f...", "user123", 3);
```

#### `async getConversationUnreadCount(conversationId, userId)`

Récupérer count pour une conversation.

```javascript
const count = await unread.getConversationUnreadCount(
  "conv_507f...",
  "user123"
);
// 5 (messages non lus)
```

#### `async getUserTotalUnread(userId)`

Total de tous les non lus d'un user.

```javascript
const total = await unread.getUserTotalUnread("user123");
// 15 (across all conversations)
```

#### `async markConversationRead(conversationId, userId)`

Marquer conversation comme lue.

```javascript
await unread.markConversationRead("conv_507f...", "user123");
// Remet le compteur à 0
```

#### `async recalculateUnread(conversationId, userId)`

Recalculer depuis la BD.

```javascript
// Si cache et BD sont désynchronisés
const actualCount = await unread.recalculateUnread("conv_507f...", "user123");
```

#### `async recalculateTotalUnread(userId)`

Recalculer total depuis la BD.

```javascript
const actualTotal = await unread.recalculateTotalUnread("user123");
```

### Pattern d'utilisation

**Réception message**

```javascript
const message = await sendMessage(...);

// Incrémenter pour tous les participants sauf sender
for (const recipientId of conversation.participants) {
  if (recipientId !== message.senderId) {
    await unread.incrementUnread(conversationId, recipientId, 1);
  }
}
```

**Lecture messages**

```javascript
// Marquer tous comme lus
await unread.markConversationRead(conversationId, userId);
```

---

## ⚙️ Configuration

### Variables d'environnement

```bash
# Connexion Redis
REDIS_HOST=localhost              # Défaut: localhost
REDIS_PORT=6379                   # Défaut: 6379
REDIS_PASSWORD=mypassword         # Défaut: undefined
REDIS_DB=0                         # Défaut: 0
REDIS_FAMILY=4                     # IPv4 ou 6

# Timeouts
REDIS_CONNECTION_TIMEOUT=5000      # 5 secondes
REDIS_MAX_RETRY_ATTEMPTS=3         # Nbr tentatives

# Modes
REDIS_KEEP_ALIVE=true              # Keep-alive socket
```

### Configuration par service

```javascript
const { RedisFactory } = require("shared/redis");

// Service 1: Chat avec cache agressif
const chatService = RedisFactory.createService("chat", {
  host: "redis-cache.internal",
  port: 6380,
  password: process.env.CACHE_PASSWORD,
  db: 1,
});

// Service 2: Auth avec TTL court
const authService = RedisFactory.createService("auth", {
  host: "redis-auth.internal",
  db: 0,
});
```

---

## 📚 Patterns d'utilisation

### Pattern 1 : Startup complet

```javascript
const express = require("express");
const { Server } = require("socket.io");
const {
  RedisManager,
  CacheService,
  OnlineUserManager,
  RoomManager,
  UnreadMessageManager,
} = require("shared/redis");

const app = express();
const io = new Server(app);

// Initialiser Redis centralement
async function setupRedis() {
  // 1. Connecter le manager
  await RedisManager.connect();
  console.log("✅ Redis connecté");

  // 2. Initialiser CacheService
  const cache = new CacheService();
  await cache.initialize(RedisManager);
  console.log("✅ Cache prêt");

  // 3. Initialiser OnlineUserManager
  const onlineUsers = new OnlineUserManager(io);
  await onlineUsers.initialize(RedisManager);
  console.log("✅ Online tracking prêt");

  // 4. Initialiser RoomManager
  const rooms = new RoomManager(io, onlineUsers);
  await rooms.initialize(RedisManager);
  console.log("✅ Rooms prêtes");

  // 5. Initialiser UnreadMessageManager
  const unread = new UnreadMessageManager();
  await unread.initialize(RedisManager);
  unread.setRecalculateFunction(MessageRepository.countUnread);
  console.log("✅ Unread tracking prêt");

  return { cache, onlineUsers, rooms, unread };
}

// Utiliser
const managers = await setupRedis();
```

### Pattern 2 : Injection dans ChatHandler

```javascript
class ChatHandler {
  constructor(
    io,
    // ... use cases ...
    cache,
    onlineUsers,
    rooms,
    unread
  ) {
    this.io = io;
    this.cache = cache;
    this.onlineUsers = onlineUsers;
    this.rooms = rooms;
    this.unread = unread;
  }

  async handleSendMessage(socket, data) {
    // Créer le message
    const message = await this.sendMessageUseCase.execute(data);

    // Mettre à jour unread
    await this.unread.incrementUnread(data.conversationId, data.receiverId, 1);

    // Émettre aux users online
    const onlineInRoom = await this.rooms.getRoomOnlineUsers(
      `conversation_${data.conversationId}`
    );

    for (const userId of onlineInRoom) {
      this.io.to(`user_${userId}`).emit("newMessage", message);
    }
  }
}
```

### Pattern 3 : Cache avec fallback

```javascript
async function getUserProfile(userId) {
  // 1. Essayer cache
  let user = await cache.get(`user:${userId}`);
  if (user) {
    console.log("✅ Cache hit");
    return user;
  }

  // 2. Fallback MongoDB
  console.log("📌 Cache miss, fetching from DB");
  user = await UserRepository.findById(userId);

  if (!user) {
    throw new Error("User not found");
  }

  // 3. Cacher pour prochainement
  await cache.set(`user:${userId}`, user, 86400);

  return user;
}
```

### Pattern 4 : Synchronisation présence

```javascript
// Dans ChatHandler authenticate
async handleAuthentication(socket, data) {
  const userId = data.userId;

  // Marquer online
  await this.onlineUsers.setUserOnline(userId, {
    socketId: socket.id,
    matricule: data.matricule,
    connectedAt: new Date(),
    lastActivity: new Date()
  });

  // Notifier les autres
  socket.broadcast.emit("user_online", { userId });
}

// Dans ChatHandler disconnect
async handleDisconnection(socket, reason) {
  const userId = socket.userId;

  // Marquer offline
  await this.onlineUsers.setUserOffline(userId);

  // Notifier les autres
  socket.broadcast.emit("user_offline", { userId });
}
```

---

## 📊 Monitoring & Métriques

### Vérifier la connexion

```javascript
const status = await RedisManager.getHealthStatus();
console.log(status);
// "OK" | "DEGRADED" | "Disconnected"
```

### Accéder aux métriques

```javascript
const metrics = RedisManager.metrics;
console.log({
  connectionsCreated: metrics.connectionsCreated,
  reconnections: metrics.reconnections,
  errors: metrics.errors,
  lastConnected: metrics.lastConnectedAt,
  lastError: metrics.lastErrorAt,
});
```

### Monitoring par client

```javascript
const client = RedisManager.getMainClient();

client.on("ready", () => console.log("Ready"));
client.on("error", (err) => console.error("Error:", err));
client.on("reconnecting", () => console.log("Reconnecting..."));
client.on("end", () => console.log("Disconnected"));
```

### Stats en temps réel

```javascript
// Users online
const onlineCount = await onlineUsers.getOnlineCount();
console.log(`${onlineCount} utilisateurs en ligne`);

// Rooms actives
const rooms = await roomManager.getAllRooms();
console.log(`${rooms.length} rooms actives`);

// Cache stats
const keys = await cache.keys("*");
console.log(`${keys.length} clés en cache`);
```

---

## 🚨 Troubleshooting

### Problème: Connexion Redis impossible

**Symptômes**

```
❌ Erreur Redis: connect ECONNREFUSED
```

**Solutions**

```bash
# 1. Vérifier Redis est lancé
redis-cli ping
# PONG

# 2. Vérifier les variables d'environnement
echo $REDIS_HOST
echo $REDIS_PORT

# 3. Vérifier la connectivité
telnet localhost 6379

# 4. Vérifier les logs Redis
tail -f /var/log/redis/redis-server.log
```

### Problème: Circuit Breaker ouvert

**Symptômes**

```
❌ Code: CIRCUIT_OPEN
```

**Solutions**

```javascript
// Vérifier l'état
console.log(RedisManager.circuitBreaker.state);
// "CLOSED" | "OPEN" | "HALF_OPEN"

// Attendre la récupération automatique
// ou forcer reset
RedisManager.circuitBreaker.reset();
```

### Problème: Clés en cache non mises à jour

**Symptômes**

```
Données anciennes renvoyées
```

**Solutions**

```javascript
// Option 1: Supprimer la clé
await cache.del("key");

// Option 2: Renouveler TTL
await cache.renewTTL("key", 3600);

// Option 3: Forcer recalcul
await unread.recalculateUnread(convId, userId);
```

### Problème: Mémoire Redis croissante

**Symptômes**

```
MEMORY USAGE croît continuellement
```

**Solutions**

```javascript
// 1. Vérifier les clés sans TTL
const keys = await RedisManager.getMainClient().keys("*");
// Ajouter TTL aux clés longues

// 2. Nettoyer les patterns obsolètes
await cache.deletePattern("old_prefix:*");

// 3. Configurer l'éviction
# Dans redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
```

---

## 📖 Ressources

- [Redis Documentation](https://redis.io/documentation)
- [node-redis Guide](https://github.com/redis/node-redis)
- [Redis Streams](https://redis.io/topics/streams)
- [Write-Ahead Logging](https://en.wikipedia.org/wiki/Write-ahead_logging)

---

**Dernière mise à jour** : 8 janvier 2026
**Version** : 1.0.0
**Auteur** : Équipe ChatApp NGOMNA



---
## Fichier d'origine : `SMARTCACHEPREWARMER_INTEGRATION.md`

# Intégration SmartCachePrewarmer avec UserStreamConsumer

## 🎯 Objectif

Le SmartCachePrewarmer dans `chat-file-service` utilise maintenant `UserStreamConsumer` pour lire les utilisateurs du stream, évitant la duplication de code et garantissant une cohérence complète.

## 📋 Changements Implémentés

### 1. **SmartCachePrewarmer.js**

**Chemin:** `chat-file-service/src/infrastructure/services/SmartCachePrewarmer.js`

#### Imports (ligne 1-6)

```javascript
const axios = require("axios");
const {
  UserCache,
  RedisManager,
  UserStreamConsumer,
} = require("../../../shared");
```

#### Constructor (ligne 40)

```javascript
this.userStreamConsumer = options.userStreamConsumer || null;
```

- **Accepte** une instance de UserStreamConsumer passée par le caller
- **Fallback** : crée une instance temporaire si non fournie

#### Méthodes Refactorisées

**`_getAllUsersFromStream()`** (ligne 115-154)

- **Cas 1** : Utilise `this.userStreamConsumer` fourni (recommandé) ✅
- **Cas 2** : Crée une instance temporaire de UserStreamConsumer
- **Fallback** : Lecture directe du stream si erreur (méthode `_readStreamDirect()`)

**`_readStreamViaConsumer(consumer = null)`** (ligne 156-210)

- Accepte une instance optionnelle de consumer
- Utilise `consumer.redis` si fourni
- Fallback à `this.userStreamConsumer.redis`
- Dernière option : `RedisManager.clients.main`
- Évite la duplication : consomme via UserStreamConsumer

**`_readStreamDirect()`** (ligne 212-260)

- Fallback ultime : lit directement le stream sans UserStreamConsumer
- Garantit la robustesse même en cas de dysfonctionnement du consumer

### 2. **index.js**

**Chemin:** `chat-file-service/src/index.js`

#### Initialisation UserStreamConsumer (ligne 243-252)

```javascript
const userStreamConsumer = new UserStreamConsumer({
  streamName: "user-service:stream:events:users",
  consumerGroup: "chat-file-service-group",
  consumerName: `chat-consumer-${process.pid}`,
  cachePrefix: "chat:cache:users:",
});
await userStreamConsumer.initialize();
await userStreamConsumer.start();
app.locals.userStreamConsumer = userStreamConsumer;
```

#### Démarrage SmartCachePrewarmer (ligne 926-937)

```javascript
const smartPrewarmer = new SmartCachePrewarmer({
  authServiceUrl: process.env.AUTH_USER_SERVICE_URL || "http://localhost:8001",
  batchSize: 500,
  delayBetweenBatches: 1500,
  maxUsers: 10000,
  streamName: "user-service:stream:events:users",
  cachePrefix: "chat:cache:users:",
  userStreamConsumer: app.locals.userStreamConsumer, // ✅ PASSER L'INSTANCE
});
```

**Avant:** SmartCachePrewarmer lisait indépendamment le stream  
**Après:** Réutilise l'instance UserStreamConsumer existante ✅

## 🔄 Architecture du Flux

```
auth-user-service
    ↓ (Publish)
user-service:stream:events:users (Redis Stream)
    ↓ (Consume)
UserStreamConsumer
    ↓ (Shared Redis client)
SmartCachePrewarmer
    ↓ (Batch processing)
chat:cache:users:* (Redis Hashes)
    ↓
UserCacheService (Lookups)
```

## ⚙️ Modes de Fonctionnement

### Mode 1: Avec Instance Fournie (Recommandé)

```javascript
// index.js crée UserStreamConsumer puis le passe à SmartCachePrewarmer
const smartPrewarmer = new SmartCachePrewarmer({
  userStreamConsumer: userStreamConsumer, // Instance existante
  // ...
});
```

**Avantages:**

- ✅ Une seule instance UserStreamConsumer en mémoire
- ✅ Pas de duplication de consumer groups
- ✅ Partage du même client Redis
- ✅ Cohérence garantie

### Mode 2: Instance Temporaire

```javascript
const smartPrewarmer = new SmartCachePrewarmer({
  // userStreamConsumer non fourni
  // ...
});
```

**Fonctionnement:**

- Crée une instance temporaire si non fournie
- Utilise un consumer group distinct: `smart-cache-prewarmer-group`
- Utilisable en mode standalone

### Mode 3: Fallback Direct

En cas d'erreur du consumer, SmartCachePrewarmer bascule automatiquement sur la lecture directe du stream.

## 📊 Statistiques & Monitoring

SmartCachePrewarmer fournit les statistiques suivantes:

```javascript
{
  totalProcessed: 1250,
  cached: 1250,
  errors: 0,
  startTime: 1708007400000,
  endTime: 1708007412000,
  duration: "12.00",
  isRunning: false
}
```

## 🧪 Tests de Validation

### Test 1: Vérifier l'Instance Partagée

```bash
# Dans le log de démarrage, vérifier:
# ✅ [SmartCachePrewarmer] Utilisation du UserStreamConsumer fourni
```

### Test 2: Vérifier la Lecture du Stream

```bash
redis-cli
> XRANGE user-service:stream:events:users - +
# Vérifier que les événements existent
> HGETALL chat:cache:users:570479H
# Vérifier que les utilisateurs sont en cache
```

### Test 3: Vérifier la Synchronisation

```bash
# Mettre à jour un utilisateur dans auth-user-service
# Vérifier que UserStreamConsumer le consomme
# Vérifier que le cache est mis à jour
```

## 🔐 Problèmes Potentiels & Solutions

### Problème: Double Consumer Group

**Symptôme:** Erreur "BUSYGROUP" au démarrage  
**Cause:** Deux instances de UserStreamConsumer avec même consumerGroup  
**Solution:** ✅ Passez l'instance existante via `options.userStreamConsumer`

### Problème: Redis Non Disponible

**Symptôme:** UserStreamConsumer non initialisé  
**Solution:** Fallback automatique à lecture directe du stream

### Problème: Stream Vide

**Symptôme:** Aucun utilisateur trouvé  
**Solution:** Fallback HTTP à `auth-user-service/all`

## 📝 Migration Notes

Pour les services existants qui créaient leur propre SmartCachePrewarmer:

**Avant:**

```javascript
const prewarmer = new SmartCachePrewarmer({
  /* ... */
});
await prewarmer.start();
```

**Après:**

```javascript
const consumer = new UserStreamConsumer({
  /* ... */
});
await consumer.initialize();
await consumer.start();

const prewarmer = new SmartCachePrewarmer({
  userStreamConsumer: consumer,
  // ...
});
await prewarmer.start();
```

## 🚀 Bénéfices de l'Intégration

✅ **Cohérence** : Une seule source de vérité pour les reads/writes du stream  
✅ **Performance** : Partage du client Redis  
✅ **Maintenabilité** : Moins de code dupliqué  
✅ **Robustesse** : Fallbacks multiples intégrés  
✅ **Scalabilité** : Peut supporter plusieurs instances sans conflits

---

**Date:** 11 février 2026  
**Service:** chat-file-service  
**Version:** v1.2.0



---
## Fichier d'origine : `RESILIENCE_DELIVERY_DOCUMENTATION.md`

# Resilience and Delivery Services

This note documents how the two Redis-based services work together:

- [chat-file-service/src/infrastructure/services/ResilientMessageService.js](chat-file-service/src/infrastructure/services/ResilientMessageService.js)
- [chat-file-service/src/infrastructure/services/MessageDeliveryService.js](chat-file-service/src/infrastructure/services/MessageDeliveryService.js)

Use this as an operator and developer guide to wire the services, understand the Redis streams they rely on, and troubleshoot delivery.

---

## Overview

- ResilientMessageService = producer side. It wraps MongoDB writes with Circuit Breaker + WAL + retries + fallback to Redis and publishes to the right Redis stream (private, group, typing, notifications, read receipts, system). It owns DLQ handling and utilities to clean or sync data.
- MessageDeliveryService = consumer side. It creates prioritized consumer groups on the same streams, delivers to connected sockets, and parks messages in a pending queue when users are offline.

Both expect a connected Redis client and Socket.IO (`io`) and are designed to run in the chat-file-service process.

---

## ResilientMessageService

### Responsibilities

- Protect MongoDB writes with `CircuitBreaker` and WAL logging before/after persistence.
- Publish messages to the correct Redis stream (private vs group) plus typing events, read receipts, notifications, and system messages.
- Handle retries, fallback to Redis when MongoDB is down, and push unrecoverable items to the DLQ.
- Provide maintenance helpers (stream stats, duplicate cleanup, full Redis purge, selective category cleaning, sync of historical MongoDB messages).
- Expose metrics and health snapshots that include worker manager status.

### Key dependencies

- `CircuitBreaker`, `StreamManager`, and `WorkerManager` come from `@chatapp-ngomna/shared`.
- A Redis client (used for streams, hashes, sorted sets) and a `messageRepository` (MongoDB) are required. `mongoConversationRepository` is optional but enables participant lookups and sync.
- `io` (Socket.IO) is optional but enables alerts and immediate emissions.

### Initialization sequence

1. Construct the service with Redis, repositories, and optional `io`.
2. Call `initializeResilienceWorkers()` to provide callbacks (default callbacks cover save/publish/dlq/notify/find/alert).
3. Call `initConsumerGroups()` to create groups for retry, DLQ, fallback, and delivery streams.
4. Start workers with `startAllWorkers()` (or `startWorkers()` for the same effect). WorkerManager handles processing, monitoring, and metrics.

### Streams and consumer groups

Stream names are defined by `StreamManager` constants and grouped as follows:

- Retry: `STREAMS.RETRY` → group `retry-workers`
- DLQ: `STREAMS.DLQ` → group `dlq-processors`
- Fallback: `STREAMS.FALLBACK` → group `fallback-workers`
- Multi-stream delivery:
  - `MULTI_STREAMS.PRIVATE` → `delivery-private`
  - `MULTI_STREAMS.GROUP` → `delivery-group`
  - `MULTI_STREAMS.TYPING` → `delivery-typing`
  - `MULTI_STREAMS.READ_RECEIPTS` → `delivery-read`
  - `MULTI_STREAMS.NOTIFICATIONS` → `delivery-notifications`
- WAL: `STREAMS.WAL` is used for pre/post write logging.

### Message ingestion path (`receiveMessage`)

1. Optionally fetch conversation participants (Mongo) to deduce receiver for private cases.
2. Write a WAL `pre_write` entry.
3. Persist via `CircuitBreaker.execute` to `messageRepository.save`.
4. Publish to the appropriate stream with participant context (private vs group determination happens here).
5. Write WAL `post_write` and clean the entry.
6. Update metrics. If Mongo save fails, the flow attempts retry, then Redis fallback (hash + `fallback` stream), and finally DLQ if unrecoverable.

### Publishing helpers

- `publishToMessageStream` chooses private vs group stream based on `receiverId` or conversation participants; it increments per-type metrics.
- `publishTypingEvent`, `publishReadReceipt`, `publishNotification`, `publishSystemMessage` push typed events into their dedicated streams.

### Recovery and cleanup

- `processRetries` replays messages from the retry stream with exponential backoff until `maxRetries`, then DLQ.
- `processFallback` replays messages stored in `fallback:*` hashes and cleans active markers.
- `processWALRecovery` scans WAL for incomplete writes older than a timeout and either reconciles or sends to DLQ.
- `removeDuplicatesFromStream` scans streams (private, group, default) and removes duplicate `messageId` entries.
- `syncExistingMessagesToStream` replays recent Mongo conversations into streams (used on startup when Redis is empty).
- `nukeAllRedisData` and `cleanRedisCategory` are operational helpers to wipe Redis data; handle with care.

### Monitoring and metrics

- `getMetrics()` merges service counters and WorkerManager metrics and exposes the circuit breaker state.
- `getStreamStats()` reports length/max/usage for every known stream.
- `monitorDLQ()` emits socket alerts if DLQ is non-empty.
- `getHealthStatus()` returns overall status, worker health, stream stats, and Redis connectivity.

---

## MessageDeliveryService

### Responsibilities

- Create consumer groups for each stream type and consume with priority ordering (typing > private > group > notifications > read receipts).
- Deliver messages to connected sockets; if a user is offline, queue the payload in a per-user pending list with TTL.
- Provide utilities to register/unregister sockets, deliver pending messages on reconnect, and diagnose delivery issues.

### Stream priority map

- typing: `stream:events:typing`, group `delivery-typing`, priority 0, interval 50 ms
- private: `stream:messages:private`, group `delivery-private`, priority 1, interval 100 ms
- group: `stream:messages:group`, group `delivery-group`, priority 2, interval 200 ms
- notifications: `stream:messages:system`, group `delivery-notifications`, priority 3, interval 500 ms
- readReceipts: `stream:events:read`, group `delivery-read`, priority 4, interval 1000 ms

### Startup sequence

1. Instantiate with Redis and `io`.
2. Call `initialize()`: duplicates Redis connections, creates consumer groups (MKSTREAM), registers each consumer, then starts all consumers by priority.
3. Consumers run `xReadGroup` with `COUNT` and `BLOCK` and acknowledge after successful delivery.

### Delivery behavior

- Private: delivers via `newMessage` to all sockets of `receiverId`, else queues in `pending:messages:<user>`.
- Group: sends to all connected participants in the conversation (excluding sender). System messages reuse `newMessage`; normal group messages use `message:group`.
- Typing: emits `typing:event` to participants except the sender.
- Read receipts: emits `read:receipt` to the original sender.
- Notifications: emits `notification:system`; if offline, queued in pending list.

### Socket and pending queue management

- `registerUserSocket(userId, socket, conversationIds)` stores socket ids and known conversations; `unregisterUserSocket` cleans up.
- `deliverPendingMessagesOnConnect(userId, socket)` drains the pending list and emits private messages, removing entries as they are delivered.
- Pending queue uses Redis list `pending:messages:<userId>` with a 24h TTL and stores serialized message payloads.

### Diagnostics and operations

- `getStats()` summarizes running state, configured streams, consumer intervals, and connected users.
- `diagnoseDelivery(userId)` inspects streams for relevant messages, pending list, conversations, and consumer groups; logs a structured summary.
- `troubleshootDelivery(userId)` auto-takes simple actions (e.g., restart consumers when inactive) based on diagnostics.
- `cleanup()` stops consumers and clears in-memory maps.

---

## Typical end-to-end flow

1. A client emits `sendMessage` through ChatHandler.
2. Use case writes via `ResilientMessageService.receiveMessage`, which WALs, persists, publishes to private/group stream, and updates metrics.
3. `MessageDeliveryService` has active consumers that read the stream entry, route it to the intended recipients, and `xAck` the entry.
4. If a recipient is offline, the delivery service adds the payload to the pending queue; it is delivered on reconnect via `deliverPendingMessagesOnConnect`.
5. Failures during persistence or publishing are retried; hard failures go to fallback or DLQ, visible through `monitorDLQ()` and stream stats.

---

## Quick usage snippets

Instantiate and wire (example):

```javascript
const resilient = new ResilientMessageService(
  redis,
  messageRepo,
  mongoMessageRepo,
  mongoConversationRepo,
  io
);
resilient.initializeResilienceWorkers();
await resilient.initConsumerGroups();
resilient.startAllWorkers();

const delivery = new MessageDeliveryService(redis, io);
await delivery.initialize();
```

On user connect:

```javascript
delivery.registerUserSocket(userId, socket, conversationIds);
await delivery.deliverPendingMessagesOnConnect(userId, socket);
```

On shutdown:

```javascript
resilient.stopAll();
await delivery.cleanup();
```

---

## Operational tips

- Keep Redis available: both services rely on streams; fallback hashes and pending lists are stored in Redis.
- Consumer groups must exist before starting delivery; `initialize()` creates them with `MKSTREAM` if missing.
- Use `getStreamStats()` and `monitorDLQ()` during incidents to spot backlog or poison messages.
- Run `removeDuplicatesFromStream()` before syncing or after incidents to avoid duplicate deliveries.
- Avoid calling `nukeAllRedisData()` outside controlled environments; it wipes all chat-related keys and consumer groups.

