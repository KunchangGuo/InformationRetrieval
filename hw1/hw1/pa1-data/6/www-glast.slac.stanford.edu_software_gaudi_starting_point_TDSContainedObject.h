ifndef tdscontainedobject_h define tdscontainedobject_h 1 include iostream include gaudi kernel kernel h include gaudi kernel containedobject h include gaudi kernel smartrefvector h include glastevent toplevel definitions h include glastevent toplevel objectvector h include glastevent toplevel objectlist h this must be defined in eventmodel cpp pick an appropriate number and make sure that it's unique extern const clid& clid_tdscontainedobject class tdscontainedobject brief this is an example class to demonstrate how to create a new tds contained object class header class tdscontainedobject virtual public containedobject public tdscontainedobject virtual tdscontainedobject virtual const clid& clid const return tdscontainedobject classid static const clid& classid return clid_myclass double value const return m_val void setvalue double value m_val value serialize the object for reading virtual streambuffer& serialize streambuffer& s serialize the object for writing virtual streambuffer& serialize streambuffer& s const fill the ascii output stream virtual std ostream& fillstream std ostream& s const private double m_value serialize the object for writing inline streambuffer& tdscontainedobject serialize streambuffer& s const containedobject serialize s return s m_value serialize the object for reading inline streambuffer& tdscontainedobject serialize streambuffer& s containedobject serialize s return s m_value fill the ascii output stream inline std ostream& tdscontainedobject fillstream std ostream& s const return s class tdscontainedobject m_value definition of all container types of tdscontainedobject class these are the containers that will go into the tds template class type class objectvector typedef objectvector tdscontainedobject tdscontainedobjectvector template class type class objectlist typedef objectlist tdscontainedobject tdscontainedobjectlist endif