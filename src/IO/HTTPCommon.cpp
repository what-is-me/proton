#include <IO/HTTPCommon.h>

#include <Server/HTTP/HTTPServerResponse.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/PoolBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

#include <Poco/Version.h>

#include "config.h"

#if USE_SSL
#    include <Poco/Net/AcceptCertificateHandler.h>
#    include <Poco/Net/Context.h>
#    include <Poco/Net/HTTPSClientSession.h>
#    include <Poco/Net/InvalidCertificateHandler.h>
#    include <Poco/Net/PrivateKeyPassphraseHandler.h>
#    include <Poco/Net/RejectCertificateHandler.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

#include <Poco/Util/Application.h>

#include <tuple>
#include <unordered_map>
#include <sstream>


namespace ProfileEvents
{
    extern const Event CreatedHTTPConnections;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int UNSUPPORTED_URI_SCHEME;
}


namespace
{
    bool isHTTPS(const Poco::URI & uri)
    {
        if (uri.getScheme() == "https")
            return true;
        else if (uri.getScheme() == "http")
            return false;
        else
            throw Exception("Unsupported scheme in URI '" + uri.toString() + "'", ErrorCodes::UNSUPPORTED_URI_SCHEME);
    }

    HTTPSessionPtr makeHTTPSessionImpl(const std::string & host, UInt16 port, bool https, bool keep_alive, Poco::Net::Context::Ptr context, bool resolve_host = true) /* proton: updated */
    {
        HTTPSessionPtr session;

        if (https)
        {
#if USE_SSL
            /// Cannot resolve host in advance, otherwise SNI won't work in Poco.
            session = std::make_shared<Poco::Net::HTTPSClientSession>(host, port, context);
#else
            throw Exception("proton was built without HTTPS support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
#endif
        }
        else
        {
            String resolved_host = resolve_host ? DNSResolver::instance().resolveHost(host).toString() : host;
            session = std::make_shared<Poco::Net::HTTPClientSession>(resolved_host, port);
        }

        ProfileEvents::increment(ProfileEvents::CreatedHTTPConnections);

        /// doesn't work properly without patch
#if defined(POCO_CLICKHOUSE_PATCH)
        session->setKeepAlive(keep_alive);
#else
        (void)keep_alive; // Avoid warning: unused parameter
#endif

        return session;
    }

    class SingleEndpointHTTPSessionPool : public PoolBase<Poco::Net::HTTPClientSession>
    {
    private:
        const std::string host;
        const UInt16 port;
        bool https;
        const String proxy_host;
        const UInt16 proxy_port;
        bool proxy_https;
        bool resolve_host;
        Poco::Net::Context::Ptr context; /* proton: updated */
        using Base = PoolBase<Poco::Net::HTTPClientSession>;
        ObjectPtr allocObject() override
        {
            auto session = makeHTTPSessionImpl(host, port, https, true, context, resolve_host);
            if (!proxy_host.empty())
            {
                const String proxy_scheme = proxy_https ? "https" : "http";
                session->setProxyHost(proxy_host);
                session->setProxyPort(proxy_port);

#if defined(POCO_CLICKHOUSE_PATCH)
                session->setProxyProtocol(proxy_scheme);

                /// Turn on tunnel mode if proxy scheme is HTTP while endpoint scheme is HTTPS.
                session->setProxyTunnel(!proxy_https && https);
#endif
            }
            return session;
        }

    public:
        SingleEndpointHTTPSessionPool(
                const std::string & host_,
                UInt16 port_,
                bool https_,
                const std::string & proxy_host_,
                UInt16 proxy_port_,
                bool proxy_https_,
                /// proton: starts
                const String & private_key_file,
                const String & certificate_file,
                const String & ca_location,
                Poco::Net::Context::VerificationMode verification_mode,
                /// proton: ends
                size_t max_pool_size_,
                bool resolve_host_ = true)
            : Base(static_cast<unsigned>(max_pool_size_), &Poco::Logger::get("HTTPSessionPool"))
            , host(host_)
            , port(port_)
            , https(https_)
            , proxy_host(proxy_host_)
            , proxy_port(proxy_port_)
            , proxy_https(proxy_https_)
            , resolve_host(resolve_host_)
            , context(new Poco::Net::Context(
                Poco::Net::SSLManager::instance().defaultClientContext()->usage(),
                private_key_file,
                certificate_file,
                ca_location,
                /*verificationMode=*/verification_mode,
                /*verificationDepth=*/9,
                /*loadDefaultCAs=*/true))
        {
        }
    };

    class HTTPSessionPool : private boost::noncopyable
    {
    public:
        struct Key
        {
            String target_host;
            UInt16 target_port;
            bool is_target_https;
            String proxy_host;
            UInt16 proxy_port;
            bool is_proxy_https;

            /// proton: starts
            String private_key_file;
            String certificate_file;
            String ca_location;
            Poco::Net::Context::VerificationMode Verification_mode;
            /// proton: ends

            bool operator ==(const Key & rhs) const
            {
                /// proton: starts
                return std::tie(target_host, target_port, is_target_https, proxy_host, proxy_port, is_proxy_https,
                        private_key_file, certificate_file, ca_location, Verification_mode)
                    == std::tie(rhs.target_host, rhs.target_port, rhs.is_target_https, rhs.proxy_host, rhs.proxy_port, rhs.is_proxy_https,
                        rhs.private_key_file, rhs.certificate_file, rhs.ca_location, rhs.Verification_mode);
                /// proton: ends
            }
        };

    private:
        using PoolPtr = std::shared_ptr<SingleEndpointHTTPSessionPool>;
        using Entry = SingleEndpointHTTPSessionPool::Entry;

        struct Hasher
        {
            size_t operator()(const Key & k) const
            {
                SipHash s;
                s.update(k.target_host);
                s.update(k.target_port);
                s.update(k.is_target_https);
                s.update(k.proxy_host);
                s.update(k.proxy_port);
                s.update(k.is_proxy_https);
                return s.get64();
            }
        };

        std::mutex mutex;
        std::unordered_map<Key, PoolPtr, Hasher> endpoints_pool;

    protected:
        HTTPSessionPool() = default;

    public:
        static auto & instance()
        {
            static HTTPSessionPool instance;
            return instance;
        }

        Entry getSession(
            const Poco::URI & uri,
            const Poco::URI & proxy_uri,
            /// proton: starts
            const String & private_key_file,
            const String & certificate_file,
            const String & ca_location,
            Poco::Net::Context::VerificationMode verification_mode,
            /// proton: ends
            const ConnectionTimeouts & timeouts,
            size_t max_connections_per_endpoint,
            bool resolve_host = true)
        {
            std::lock_guard lock(mutex);
            const std::string & host = uri.getHost();
            UInt16 port = uri.getPort();
            bool https = isHTTPS(uri);


            String proxy_host;
            UInt16 proxy_port = 0;
            bool proxy_https = false;
            if (!proxy_uri.empty())
            {
                proxy_host = proxy_uri.getHost();
                proxy_port = proxy_uri.getPort();
                proxy_https = isHTTPS(proxy_uri);
            }

            HTTPSessionPool::Key key{
                host, port, https, proxy_host, proxy_port, proxy_https,
                private_key_file, certificate_file, ca_location, verification_mode};
            auto pool_ptr = endpoints_pool.find(key);
            if (pool_ptr == endpoints_pool.end())
                std::tie(pool_ptr, std::ignore) = endpoints_pool.emplace(
                    key, std::make_shared<SingleEndpointHTTPSessionPool>(
                        host, port, https, proxy_host, proxy_port, proxy_https,
                        private_key_file, certificate_file, ca_location, verification_mode, /* proton: updated */
                        max_connections_per_endpoint, resolve_host));

            auto retry_timeout = timeouts.connection_timeout.totalMicroseconds();
            auto session = pool_ptr->second->get(retry_timeout);

            /// We store exception messages in session data.
            /// Poco HTTPSession also stores exception, but it can be removed at any time.
            const auto & session_data = session->sessionData();
            if (!session_data.empty())
            {
                auto msg = Poco::AnyCast<std::string>(session_data);
                if (!msg.empty())
                {
                    LOG_TRACE((&Poco::Logger::get("HTTPCommon")), "Failed communicating with {} with error '{}' will try to reconnect session", host, msg);

                    if (resolve_host)
                    {
                        /// Host can change IP
                        const auto ip = DNSResolver::instance().resolveHost(host).toString();
                        if (ip != session->getHost())
                        {
                            session->reset();
                            session->setHost(ip);
                            session->attachSessionData({});
                        }
                    }
                }
            }

            setTimeouts(*session, timeouts);

            return session;
        }
    };
}

void setResponseDefaultHeaders(HTTPServerResponse & response, size_t keep_alive_timeout)
{
    if (!response.getKeepAlive())
        return;

    Poco::Timespan timeout(keep_alive_timeout, 0);
    if (timeout.totalSeconds())
        response.set("Keep-Alive", "timeout=" + std::to_string(timeout.totalSeconds()));
}

HTTPSessionPtr makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, bool resolve_host)
{
    const std::string & host = uri.getHost();
    UInt16 port = uri.getPort();
    bool https = isHTTPS(uri);

    auto session = makeHTTPSessionImpl(host, port, https, false, Poco::Net::SSLManager::instance().defaultClientContext(), resolve_host);
    setTimeouts(*session, timeouts);
    return session;
}

/// proton: starts
PooledHTTPSessionPtr makePooledHTTPSession(
    const Poco::URI & uri,
    const String & private_key_file,
    const String & certificate_file,
    const String & ca_location,
    Poco::Net::Context::VerificationMode verification_mode,
    const ConnectionTimeouts & timeouts,
    size_t per_endpoint_pool_size,
    bool resolve_host)
{
    return makePooledHTTPSession(uri, {},
        private_key_file, certificate_file, ca_location, verification_mode,
        timeouts, per_endpoint_pool_size, resolve_host);
}

PooledHTTPSessionPtr makePooledHTTPSession(
    const Poco::URI & uri,
    const Poco::URI & proxy_uri,
    const String & private_key_file,
    const String & certificate_file,
    const String & ca_location,
    Poco::Net::Context::VerificationMode verification_mode,
    const ConnectionTimeouts & timeouts,
    size_t per_endpoint_pool_size,
    bool resolve_host)
{
    return HTTPSessionPool::instance().getSession(uri, proxy_uri,
        private_key_file, certificate_file, ca_location, verification_mode,
        timeouts, per_endpoint_pool_size, resolve_host);
}
/// proton: ends


PooledHTTPSessionPtr makePooledHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts, size_t per_endpoint_pool_size, bool resolve_host)
{
    return makePooledHTTPSession(uri, {}, timeouts, per_endpoint_pool_size, resolve_host);
}

PooledHTTPSessionPtr makePooledHTTPSession(
    const Poco::URI & uri,
    const Poco::URI & proxy_uri,
    const ConnectionTimeouts & timeouts,
    size_t per_endpoint_pool_size,
    bool resolve_host)
{
    return HTTPSessionPool::instance().getSession(uri, proxy_uri,
        /*private_key_file=*/"", /*certificate_file=*/"", /*ca_location=*/"", /*verification_mode=*/Poco::Net::Context::VERIFY_RELAXED,
            timeouts, per_endpoint_pool_size, resolve_host);
}

bool isRedirect(const Poco::Net::HTTPResponse::HTTPStatus status) { return status == Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY  || status == Poco::Net::HTTPResponse::HTTP_FOUND || status == Poco::Net::HTTPResponse::HTTP_SEE_OTHER  || status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT; }

std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, const bool allow_redirects)
{
    auto & istr = session.receiveResponse(response);
    assertResponseIsOk(request, response, istr, allow_redirects);
    return &istr;
}

void assertResponseIsOk(const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response, std::istream & istr, const bool allow_redirects)
{
    auto status = response.getStatus();

    if (!(status == Poco::Net::HTTPResponse::HTTP_OK
        || status == Poco::Net::HTTPResponse::HTTP_CREATED
        || status == Poco::Net::HTTPResponse::HTTP_ACCEPTED
        || status == Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT /// Reading with Range header was successful.
        || (isRedirect(status) && allow_redirects)))
    {
        int code = status == Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS
            ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
            : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;

        std::stringstream body; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        body.exceptions(std::ios::failbit);
        body << istr.rdbuf();

        throw HTTPException(code, request.getURI(), status, response.getReason(), body.str());
    }
}

void setTimeouts(Poco::Net::HTTPClientSession & session, const ConnectionTimeouts & timeouts)
{
    session.setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
    session.setKeepAliveTimeout(timeouts.http_keep_alive_timeout);
}

std::string HTTPException::makeExceptionMessage(
    const std::string & uri,
    Poco::Net::HTTPResponse::HTTPStatus http_status,
    const std::string & reason,
    const std::string & body)
{
    return fmt::format(
        "Received error from remote server {}. "
        "HTTP status code: {} {}, "
        "body: {}",
        uri, http_status, reason, body);
}

}
